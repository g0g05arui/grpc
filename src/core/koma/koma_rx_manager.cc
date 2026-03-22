#include "koma_rx_manager.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <memory>
#include <functional>
#include <thread>
#include <iostream>
#include "absl/status/status.h"
#include "koma_common.h"

koma_rx_manager::~koma_rx_manager() { shutdown(); }

absl::Status koma_rx_manager::start() {

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
void koma_rx_manager::on_accepted_tcp(int fd) {
    absl::MutexLock lock(&m_mu);
    if(m_workers.empty()) return; // maybe should give an error

    const size_t idx = m_next_worker++ % m_workers.size();
    koma_worker* worker = m_workers[idx].get();
    {
        absl::MutexLock pending_lock(&worker->pending_mu);
        worker->pending_tcp_fds.push_back(fd);
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

    std::deque<int> pending;
    {
        absl::MutexLock pending_lock(&worker->pending_mu);
        pending.swap(worker->pending_tcp_fds);
    }
    while (!pending.empty()) {
        int tcp_fd = pending.front();
        pending.pop_front();
        if (koma_attach(worker->koma_fd, tcp_fd) < 0) {
            std::cout << "koma_attach failed for fd " << tcp_fd << '\n';
            close(tcp_fd);
        }
        std::cout << "Attacahed tcp fd #" << tcp_fd << '\n';
    }
}

void koma_rx_manager::handle_worker_komafd(koma_rx_manager::koma_worker* worker) {
    if (koma_pull(worker->koma_fd) < 0) {
        return;
    }

    while (true) {
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

        std::cout << "Received " << n << " bytes\n";

    }
}

void koma_rx_manager::cleanup(koma_rx_manager::koma_worker& worker) {
    std::cout << "Cleanup worker" << worker.id << '\n';
    if (worker.event_fd >= 0) close(worker.event_fd);
    if (worker.epoll_fd >= 0) close(worker.epoll_fd);
    if (worker.koma_fd >= 0) close(worker.koma_fd);
}
