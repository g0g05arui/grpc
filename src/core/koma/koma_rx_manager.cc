#include "koma_rx_manager.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <memory>
#include <functional>
#include <thread>
#include "absl/status/status.h"
#include "src/core/koma/koma_common.h"

absl::Status koma_rx_manager::start() {

    absl::MutexLock lock(&m_mu);
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
        worker->epoll_fd = epoll_create1(EFD_CLOEXEC);
        worker->event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        if(worker->epoll_fd < 0 || worker->event_fd < 0){
            cleanup(*worker);
            return absl::InternalError("epoll or eventfd create failed");
        }
        worker->thread = std::thread(&koma_rx_manager::worker_loop, this, std::cref(*worker));
        m_workers.emplace_back(std::move(worker));
    }
    return absl::OkStatus();
}
void koma_rx_manager::on_accepted_tcp(int fd) {
    if(m_workers.empty()) return; // maybe should give an error
    koma_attach(m_workers.front()->koma_fd, fd); // attach to the first / any worker
}

void koma_rx_manager::worker_loop(const koma_rx_manager::koma_worker& worker) {
    // TODO(mihai) start read loop
}

void koma_rx_manager::cleanup(const koma_rx_manager::koma_worker& worker) {
    // TODO(mihai) cleanup worker resources
}
