#pragma once

#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
#include <atomic>
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

class koma_rx_manager {


public:
    explicit koma_rx_manager(size_t num_threads) : m_num_threads(num_threads){};
    ~koma_rx_manager();



    absl::Status start();

    void shutdown();

    void on_accepted_tcp(int tcp_fd);


private:

    struct koma_worker {
        size_t id = 0;
        int koma_fd = -1;
        std::atomic<bool> stopped{false};
        // std::atomic<int> attached_tcp_connections{0};
        //  ; this would ne necessary
        //  if we needed to load balance, but koma socks can read from any associated fd
        int epoll_fd = -1;
        int event_fd = -1;
        std::thread thread;
    };

    void worker_loop(const koma_worker& worker);

    void cleanup(const koma_worker& worker);

    size_t m_num_threads;
    std::vector<std::unique_ptr<koma_worker>> m_workers ABSL_GUARDED_BY(m_mu);
    std::unordered_map<int, int> m_tcp_fd_to_worker;
    absl::Mutex m_mu;
};
