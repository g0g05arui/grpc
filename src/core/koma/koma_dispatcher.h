#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include "absl/strings/string_view.h"

using koma_handler = std::function<std::string(const uint8_t*, size_t)>;

class koma_dispatcher {
public:
    virtual ~koma_dispatcher() = default;
    virtual koma_handler* find_handler(absl::string_view path) = 0;
};
