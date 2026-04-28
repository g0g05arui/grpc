#pragma once

#include "absl/strings/string_view.h"
#include <grpcpp/impl/rpc_service_method.h>

class koma_dispatcher {
public:
    virtual ~koma_dispatcher() = default;
    virtual koma_handler* find_handler(absl::string_view path) = 0;
};
