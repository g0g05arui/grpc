#pragma once

#include <array>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <deque>
#include <sys/socket.h>
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "src/core/koma/koma_dispatcher.h"
#include "src/core/ext/transport/chttp2/transport/hpack_parser.h"
#include "koma_common.h"

struct epoll_event;

class koma_rx_manager {


public:

    struct pending_conn{
        int attach_fd;
        int write_fd;
    };

    explicit koma_rx_manager(size_t num_threads = NUM_KOMA_SOCKETS) : m_num_threads(num_threads){};
    ~koma_rx_manager();



    absl::Status start();

    void shutdown();

    void on_accepted_tcp(pending_conn conn);

    void set_dispatcher(koma_dispatcher * d);


private:

    static constexpr int MAX_MSG_SIZE = 4 * 1024 * 1024 + 96;

    koma_dispatcher* dispatcher;

    struct koma_worker {

        size_t id = 0;
        int koma_fd = -1;
        std::atomic<bool> stopped{false};
        // std::atomic<int> attached_tcp_connections{0};
        //  ; this would ne necessary
        //  if we needed to load balance, but koma socks can read from any associated fd
        int epoll_fd = -1;
        int event_fd = -1;
        absl::Mutex pending_mu;
        std::deque<pending_conn> pending_tcp_fds ABSL_GUARDED_BY(pending_mu);
        std::thread thread;

        std::array<uint8_t, MAX_MSG_SIZE> recv_buf;
        grpc_core::HPackParser parser;
    };

    void worker_loop(koma_worker* worker);
    void handle_worker_events(koma_worker* worker, const struct epoll_event* events, int nfds);
    void handle_worker_eventfd(koma_worker* worker);
    void handle_worker_komafd(koma_worker* worker);

    void cleanup(koma_worker& worker);

    size_t m_num_threads;
    std::vector<std::unique_ptr<koma_worker>> m_workers ABSL_GUARDED_BY(m_mu);
    size_t m_next_worker ABSL_GUARDED_BY(m_mu) = 0;
    // std::unordered_map<int, int> m_tcp_fd_to_worker;
    absl::Mutex m_mu;
};
