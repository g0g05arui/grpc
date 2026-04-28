#include "koma_rx_manager.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <functional>
#include <thread>
#include <iostream>
#include "absl/random/bit_gen_ref.h"
#include "absl/status/status.h"
#include "src/core/koma/koma_dispatcher.h"
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
        close(conn);
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

    epoll_event events[8];
    while (!worker->stopped) {
        koma_pull(worker->koma_fd);
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

    static const uint8_t server_preface[37] = {
        0x00, 0x00, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,  // SETTINGS
        0x00, 0x04, 0x00, 0x40, 0x00, 0x00,                    // INITIAL_WINDOW_SIZE = 4MB
        0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00,  // SETTINGS ACK
        0x00, 0x00, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,  // WINDOW_UPDATE
        0x7f, 0xff, 0xff, 0xff,
    };

    for (auto& conn : pending) {
        // read preface
        char buf[24] = {0};
        ssize_t recvd = 0;
        while (recvd < 24) {
            ssize_t r = recv(conn, buf + recvd, 24 - recvd, 0);
            if (r > 0) { recvd += r; continue; }
            if (r < 0 && errno == EAGAIN) continue;
            std::cout << "Preface recv failed after " << recvd << " bytes\n";
            break;
        }

        if (recvd == 24) {
            if (koma_attach(worker->koma_fd, conn) < 0) {
                std::cout << "koma_attach failed for fd " << conn << '\n';
            } else {
                ssize_t sent = send(conn, server_preface, sizeof(server_preface), MSG_NOSIGNAL);
                if (sent < 0) {
                    std::cout << "Failed to send server preface to fd " << conn
                              << ": " << strerror(errno) << '\n';
                } else {
                    std::cout << "Attached tcp fd #" << conn
                              << " to worker " << worker->id << '\n';
                }
            }
        }

        close(conn);
    }
}

void koma_rx_manager::handle_worker_komafd(koma_rx_manager::koma_worker* worker) {
    iovec iov{};
    iov.iov_base = worker->recv_buf.data();
    iov.iov_len = worker->recv_buf.size();
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    ssize_t n = recvmsg(worker->koma_fd, &msg, 0);
    if (n <= 0) {
        return;
    }

    std::cout << "Received " << n << " bytes\n";

    const uint8_t* p = worker->recv_buf.data();

    auto header = grpc_core::Http2FrameHeader::Parse(p);

    if (header.type != 0x1 || header.length + 18 > n) {
        return;
    }
    p += 9;
    grpc_core::SliceBuffer hpack_payload;
    hpack_payload.Append(grpc_core::Slice::FromCopiedBuffer(p, header.length));
    p += header.length;

    grpc_metadata_batch metadata;
    hpack_decode(worker->parser, hpack_payload, false, metadata);

    const uint8_t* end = worker->recv_buf.data() + n;
    uint8_t grpc_hdr[5];
    size_t hdr_len = 0;
    size_t proto_len = 0;
    size_t expected_proto_len = 0;
    koma_payload payload;

    while (p + 9 <= end) {
        grpc_core::Http2FrameHeader data_hdr = grpc_core::Http2FrameHeader::Parse(p);
        p += 9;
        if (p + data_hdr.length > end) return;

        if (data_hdr.type == 0x0) {
            absl::string_view data(reinterpret_cast<const char*>(p), data_hdr.length);
            size_t off = 0;
            while (hdr_len < sizeof(grpc_hdr) && off < data.size()) {
                grpc_hdr[hdr_len++] = data[off++];
            }
            if (hdr_len == sizeof(grpc_hdr)) {
                if (expected_proto_len == 0) {
                    expected_proto_len = (static_cast<uint32_t>(grpc_hdr[1]) << 24) |
                                         (static_cast<uint32_t>(grpc_hdr[2]) << 16) |
                                         (static_cast<uint32_t>(grpc_hdr[3]) << 8) |
                                         static_cast<uint32_t>(grpc_hdr[4]);
                }
                if (off < data.size()) {
                    size_t len = std::min(data.size() - off, expected_proto_len - proto_len);
                    payload.push_back(data.substr(off, len));
                    proto_len += len;
                }
            }
        }

        p += data_hdr.length;
    }

    if (hdr_len != sizeof(grpc_hdr) || proto_len != expected_proto_len) return;
    absl::Status status = dispatch(worker, msg, payload, metadata, header);

    if(status != absl::OkStatus()){
        std::cerr << status.message() << '\n';
    }
}

void koma_rx_manager::cleanup(koma_rx_manager::koma_worker& worker) {
    std::cout << "Cleanup worker" << worker.id << '\n';
    if (worker.event_fd >= 0) close(worker.event_fd);
    if (worker.epoll_fd >= 0) close(worker.epoll_fd);
    if (worker.koma_fd >= 0) close(worker.koma_fd);
}

void koma_rx_manager::set_dispatcher(koma_dispatcher *d){
  dispatcher = d;
}

absl::Status koma_rx_manager::dispatch(const koma_worker * worker,
                                        msghdr& msg,
                                        const koma_payload &payload,
                                        const grpc_metadata_batch &metadata,
                                        grpc_core::Http2FrameHeader & header
){

    const grpc_core::Slice* path_slice = metadata.get_pointer(grpc_core::HttpPathMetadata());
    if (path_slice == nullptr) return absl::InternalError("null path slice");
    absl::string_view path = path_slice->as_string_view();

    // Look up handler
    if (dispatcher == nullptr) return absl::InternalError("dispatcher is null");
    koma_handler* handler = dispatcher->find_handler(path);
    if (handler == nullptr) {
        return absl::NotFoundError("path not found");
    }
    std::string response_proto = (*handler)(payload);
    static const uint8_t content_type[] = {
        0x88,
        0x40, 0x0c, 'c','o','n','t','e','n','t','-','t','y','p','e',
              0x10, 'a','p','p','l','i','c','a','t','i','o','n','/','g','r','p','c',
    };
    static const uint8_t status[] = {
        0x40, 0x0b, 'g','r','p','c','-','s','t','a','t','u','s',
              0x01, '0',
    };

    uint32_t resp_size = response_proto.size();
    uint32_t data_frame_len = 5 + resp_size;

    std::vector<uint8_t> resp(27 + sizeof(content_type) + data_frame_len + sizeof(status));
    uint8_t* out = resp.data();
    uint32_t stream = header.stream_id;
    grpc_core::Http2FrameHeader{sizeof(content_type), 0x1, 0x4, stream}.Serialize(out);
    out += 9;
    memcpy(out, content_type, sizeof(content_type));
    out += sizeof(content_type);

    // DATA frame
    grpc_core::Http2FrameHeader{data_frame_len, 0x0, 0x0, stream}.Serialize(out);
    out += 9;
    out[0] = 0;  // no compression flag
    out[1] = (resp_size >> 24) & 0xff;
    out[2] = (resp_size >> 16) & 0xff;
    out[3] = (resp_size >> 8) & 0xff;
    out[4] =  resp_size & 0xff;
    out += 5;
    memcpy(out, response_proto.data(), resp_size);
    out += resp_size;

    grpc_core::Http2FrameHeader{sizeof(status), 0x1, 0x5, stream}.Serialize(out);
    out += 9;
    memcpy(out, status, sizeof(status));

    iovec resp_iov{};
    resp_iov.iov_base = resp.data();
    resp_iov.iov_len = resp.size();
    msg.msg_iov = &resp_iov;
    msg.msg_iovlen = 1;
    ssize_t sent = sendmsg(worker->koma_fd, &msg, 0);
    if (sent < 0) {
        return absl::InternalError("koma sendmsg failed");
    }
    return absl::OkStatus();
}
