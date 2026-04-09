#pragma once
#include <functional>
#include "lib/slice/slice.h"

using koma_handler = std::function<grpc_core::Slice(const uint8_t*, size_t)>;

class koma_dispatcher {
public:
    virtual ~koma_dispatcher() = default;
    virtual koma_handler* find_handler(absl::string_view path) = 0;
};
