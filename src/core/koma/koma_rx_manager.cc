#include "koma_rx_manager.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <cstring>
#include <memory>
#include <functional>
#include <thread>
#include <iostream>
#include "absl/random/bit_gen_ref.h"
#include "absl/status/status.h"
#include "src/core/call/metadata_batch.h"
#include "src/core/ext/transport/chttp2/transport/frame.h"
#include "src/core/ext/transport/chttp2/transport/hpack_parser.h"
#include "koma_common.h"
#include "src/core/util/shared_bit_gen.h"

static void hpack_decode(grpc_core::HPackParser& parser, grpc_core::SliceBuffer& payload,
                           bool end_stream, grpc_metadata_batch& out) {
      grpc_core::SharedBitGen bitgen;
      parser.BeginFrame(
          &out,
          16 * 1024,
          64 * 1024,
          end_stream ? grpc_core::HPackParser::Boundary::EndOfStream
                     : grpc_core::HPackParser::Boundary::EndOfHeaders,
          grpc_core::HPackParser::Priority::None,
          {
            0, grpc_core::HPackParser::LogInfo::kHeaders,
           false
          }
      );

      for (size_t i = 0; i < payload.Count(); ++i) {
          grpc_core::Slice s = payload.RefSlice(i);
          bool is_last = (i == payload.Count() - 1);
          (void)parser.Parse(s.c_slice(), is_last, absl::BitGenRef(bitgen), nullptr); // TODO(mihai) : here I should check for the returned error
      }
     parser.FinishFrame();
  }

koma_rx_manager::~koma_rx_manager() { shutdown(); }

absl::Status koma_rx_manager::start() {
    signal(SIGPIPE, SIG_IGN);

    absl::MutexLock lock(&m_mu);
    std::cout << "Starting koma rx manager" << std::endl;
    if(!m_workers.empty()){
        return absl::Status(absl::StatusCode::kAlreadyExists, "already started");
    }
    m_workers.reserve(m_num_threads);
    for(size_t i = 0; i < m_num_threads; ++i){
        auto worker = std::make_unique<koma_worker>();
        worker->id = i;
        worker->koma_fd = koma_init();

        if(worker->koma_fd < 0){
            return absl::InternalError("koma init failed");
        }
        worker->epoll_fd = epoll_create1(0);
        worker->event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        if(worker->epoll_fd < 0 || worker->event_fd < 0){
            cleanup(*worker);
            return absl::InternalError("epoll or eventfd create failed");
        }
        int flags = fcntl(worker->koma_fd, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(worker->koma_fd, F_SETFL, flags | O_NONBLOCK);
        }
        worker->thread = std::thread(&koma_rx_manager::worker_loop, this, worker.get());
        m_workers.emplace_back(std::move(worker));
    }
    return absl::OkStatus();
}
void koma_rx_manager::on_accepted_tcp(pending_conn conn) {
    absl::MutexLock lock(&m_mu);
    if (m_workers.empty()) {
        close(conn.attach_fd);
        close(conn.write_fd);
        return;
    }
    koma_worker* worker = m_workers[m_next_worker++ % m_workers.size()].get();
    {
        absl::MutexLock pending_lock(&worker->pending_mu);
        worker->pending_tcp_fds.push_back(conn);
    }
    const uint64_t one = 1;
    (void)write(worker->event_fd, &one, sizeof(one));
}

void koma_rx_manager::shutdown() {
    absl::MutexLock lock(&m_mu);
    for (auto& worker : m_workers) {
        worker->stopped = true;
        const uint64_t one = 1;
        (void)write(worker->event_fd, &one, sizeof(one));
    }
    for (auto& worker : m_workers) {
        if (worker->thread.joinable()) worker->thread.join();
        cleanup(*worker);
    }
    m_workers.clear();
}

void koma_rx_manager::worker_loop(koma_rx_manager::koma_worker* worker) {
    std::cout << "Started worker Loop for " << worker->id << '\n';

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLERR;
    ev.data.fd = worker->event_fd;
    if (epoll_ctl(worker->epoll_fd, EPOLL_CTL_ADD, worker->event_fd, &ev) < 0) {
        std::cout << "Failed to add eventfd to epoll for worker " << worker->id << '\n';
        return;
    }

    ev.data.fd = worker->koma_fd;
    if (epoll_ctl(worker->epoll_fd, EPOLL_CTL_ADD, worker->koma_fd, &ev) < 0) {
        std::cout << "Failed to add komafd to epoll for worker " << worker->id << '\n';
        return;
    }

    koma_pull(worker->koma_fd);

    epoll_event events[8];
    while (!worker->stopped) {
        const int nfds = epoll_wait(worker->epoll_fd, events, 8, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            std::cout << "epoll_wait failed for worker " << worker->id << '\n';
            return;
        }
        handle_worker_events(worker, events, nfds);
    }
}

void koma_rx_manager::handle_worker_events(koma_rx_manager::koma_worker* worker,
                                           const epoll_event* events, int nfds) {
    for (int i = 0; i < nfds; ++i) {
        const int fd = events[i].data.fd;
        if (fd == worker->event_fd) {
            handle_worker_eventfd(worker);
        } else if (fd == worker->koma_fd) {
            handle_worker_komafd(worker);
        }
    }
}

void koma_rx_manager::handle_worker_eventfd(koma_rx_manager::koma_worker* worker) {
    uint64_t counter = 0;
    (void)read(worker->event_fd, &counter, sizeof(counter));

    std::deque<pending_conn> pending;
    {
        absl::MutexLock pending_lock(&worker->pending_mu);
        pending.swap(worker->pending_tcp_fds);
    }

    static const uint8_t server_preface[18] = {
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,  // SETTINGS
        0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00,  // SETTINGS ACK
    };

    for (auto& conn : pending) {
        // read preface
        char buf[24] = {0};
        ssize_t recvd = 0;
        while (recvd < 24) {
            ssize_t r = recv(conn.attach_fd, buf + recvd, 24 - recvd, 0);
            if (r > 0) { recvd += r; continue; }
            if (r < 0 && errno == EAGAIN) continue;
            std::cout << "Preface recv failed after " << recvd << " bytes\n";
            break;
        }

        if (recvd == 24) {
            if (koma_attach(worker->koma_fd, conn.attach_fd) < 0) {
                std::cout << "koma_attach failed for fd " << conn.attach_fd << '\n';
            } else {
                ssize_t sent = send(conn.write_fd, server_preface, sizeof(server_preface), MSG_NOSIGNAL);
                if (sent < 0) {
                    std::cout << "Failed to send server preface to fd " << conn.write_fd
                              << ": " << strerror(errno) << '\n';
                } else {
                    std::cout << "Attached tcp fd #" << conn.attach_fd
                              << " to worker " << worker->id << '\n';
                }
            }
        }

        close(conn.attach_fd);
        close(conn.write_fd);  // TODO(mihai): keep write_fd alive once response sending is implemented
    }
}

void koma_rx_manager::handle_worker_komafd(koma_rx_manager::koma_worker* worker) {
    if (koma_pull(worker->koma_fd) < 0) {
        return;
    }

    while (true) { // read "in abyss", basically discard all data
        char buf[4096];
        iovec iov{};
        iov.iov_base = buf;
        iov.iov_len = sizeof(buf);
        msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        ssize_t n = recvmsg(worker->koma_fd, &msg, 0);
        if (n <= 0) {
            break;
        }

        //TODO(mihai) check that this doesn't break / works correctly and refactor a bit once done, cause this function is kinda ugly

        const uint8_t* p = worker->recv_buf.data();

        auto header = grpc_core::Http2FrameHeader::Parse(p);
        p += 9;
        grpc_core::SliceBuffer hpack_payload;
        hpack_payload.Append(grpc_core::Slice::FromCopiedBuffer(p, header.length));
        p += header.length;

        grpc_metadata_batch metadata;
        hpack_decode(worker->parser, hpack_payload, false, metadata);

        grpc_core::Http2FrameHeader data_hdr = grpc_core::Http2FrameHeader::Parse(p);
        p += 9;

        grpc_core::SliceBuffer data_payload;
        data_payload.Append(grpc_core::Slice::FromCopiedBuffer(p, data_hdr.length));

        auto grpc_hdr = grpc_core::ExtractGrpcHeader(data_payload);

        //TODO(mihai) : see how to dispatch this correctly
        // how they do it now is: with a grpc_call pointer from a CQ, but we no longer have this
        // and no grpc_call ptr, so I might need to use a map from method path to actual handler
        // migh need  to create a KomaDispatcher class for this logic

        std::cout << "Received " << n << " bytes\n";
    }
}

void koma_rx_manager::cleanup(koma_rx_manager::koma_worker& worker) {
    std::cout << "Cleanup worker" << worker.id << '\n';
    if (worker.event_fd >= 0) close(worker.event_fd);
    if (worker.epoll_fd >= 0) close(worker.epoll_fd);
    if (worker.koma_fd >= 0) close(worker.koma_fd);
}
