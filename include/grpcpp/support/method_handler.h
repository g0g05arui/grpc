//
//
// Copyright 2015 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

#ifndef GRPCPP_SUPPORT_METHOD_HANDLER_H
#define GRPCPP_SUPPORT_METHOD_HANDLER_H

#include <grpc/byte_buffer.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/sync_stream.h>

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include "absl/log/absl_check.h"

namespace grpc {

namespace internal {

inline bool WriteKomaResponseBytes(
    const uint8_t* bytes, size_t len,
    google::protobuf::io::ZeroCopyOutputStream* output) {
  const uint8_t* src = bytes;
  while (len > 0) {
    void* data = nullptr;
    int size = 0;
    if (!output->Next(&data, &size)) return false;
    size_t writable = size;
    const size_t n = std::min(len, writable);
    std::memcpy(data, src, n);
    if (n < writable) {
      int backup = writable - n;
      output->BackUp(backup);
    }
    src += n;
    len -= n;
  }
  return true;
}

// Invoke the method handler, fill in the status, and
// return whether or not we finished safely (without an exception).
// Note that exception handling is 0-cost in most compiler/library
// implementations (except when an exception is actually thrown),
// so this process doesn't require additional overhead in the common case.
// Additionally, we don't need to return if we caught an exception or not;
// the handling is the same in either case.
template <class Callable>
::grpc::Status CatchingFunctionHandler(Callable&& handler) {
#if GRPC_ALLOW_EXCEPTIONS
  try {
    return handler();
  } catch (...) {
    return grpc::Status(grpc::StatusCode::UNKNOWN,
                        "Unexpected error in RPC handling");
  }
#else   // GRPC_ALLOW_EXCEPTIONS
  return handler();
#endif  // GRPC_ALLOW_EXCEPTIONS
}

/// A helper function with reduced templating to do the common work needed to
/// actually send the server response. Uses non-const parameter for Status since
/// this should only ever be called from the end of the RunHandler method.

template <class ResponseType>
void UnaryRunHandlerHelper(const MethodHandler::HandlerParameter& param,
                           ResponseType* rsp, grpc::Status& status) {
  ABSL_CHECK(!param.server_context->sent_initial_metadata_);
  grpc::internal::CallOpSet<grpc::internal::CallOpSendInitialMetadata,
                            grpc::internal::CallOpSendMessage,
                            grpc::internal::CallOpServerSendStatus>
      ops;
  ops.SendInitialMetadata(&param.server_context->initial_metadata_,
                          param.server_context->initial_metadata_flags());
  if (param.server_context->compression_level_set()) {
    ops.set_compression_level(param.server_context->compression_level());
  }
  if (status.ok()) {
    status = ops.SendMessagePtr(rsp, param.server_context->memory_allocator());
  }
  ops.ServerSendStatus(&param.server_context->trailing_metadata_, status);
  ops.FillOps(param.call);
  param.call->cq()->Pluck(&ops);
}

/// A helper function with reduced templating to do deserializing.

template <class RequestType>
void* UnaryDeserializeHelper(grpc_byte_buffer* req, grpc::Status* status,
                             RequestType* request) {
  grpc::ByteBuffer buf;
  buf.set_buffer(req);
  *status = grpc::Deserialize(&buf, static_cast<RequestType*>(request));
  buf.Release();
  if (status->ok()) {
    return request;
  }
  request->~RequestType();
  return nullptr;
}

/// A wrapper class of an application provided rpc method handler.
template <class ServiceType, class RequestType, class ResponseType,
          class BaseRequestType = RequestType,
          class BaseResponseType = ResponseType>
class RpcMethodHandler : public grpc::internal::MethodHandler {
 public:
  RpcMethodHandler(
      std::function<grpc::Status(ServiceType*, grpc::ServerContext*,
                                 const RequestType*, ResponseType*)>
          func,
      ServiceType* service)
      : func_(func), service_(service) {}
  koma_handler to_koma_handler() override {
    return [this](const koma_payload& body,
                  google::protobuf::io::ZeroCopyOutputStream* output) -> bool {
        RequestType req;
        if (body.size() == 1) {
            if (!req.ParseFromArray(body[0].data(), body[0].size())) {
                return false;
            }
        } else {
            std::vector<std::unique_ptr<google::protobuf::io::ArrayInputStream>> arrays;
            std::vector<google::protobuf::io::ZeroCopyInputStream*> streams;
            arrays.reserve(body.size());
            streams.reserve(body.size());
            for (absl::string_view frag : body) {
                arrays.emplace_back(std::make_unique<google::protobuf::io::ArrayInputStream>(
                    frag.data(), frag.size()));
                streams.push_back(arrays.back().get());
            }
            google::protobuf::io::ConcatenatingInputStream input(streams.data(),
                                                                streams.size());
            if (!req.ParseFromZeroCopyStream(&input)) {
                return false;
            }
        }
        ResponseType resp;
        grpc::ServerContext ctx;
        (void)func_(service_, &ctx, &req, &resp);

        const size_t resp_size = resp.ByteSizeLong();
        if (resp_size > std::numeric_limits<uint32_t>::max()) {
          return false;
        }
        const uint32_t wire_size = resp_size;
        uint8_t grpc_hdr[5];
        grpc_hdr[0] = 0;
        grpc_hdr[1] = (wire_size >> 24) & 0xff;
        grpc_hdr[2] = (wire_size >> 16) & 0xff;
        grpc_hdr[3] = (wire_size >> 8) & 0xff;
        grpc_hdr[4] = wire_size & 0xff;
        return WriteKomaResponseBytes(grpc_hdr, sizeof(grpc_hdr), output) &&
               resp.SerializeToZeroCopyStream(output);
    };
  }
  void RunHandler(const HandlerParameter& param) final {
    ResponseType rsp;
    grpc::Status status = param.status;
    if (status.ok()) {
      status = CatchingFunctionHandler([this, &param, &rsp] {
        return func_(service_,
                     static_cast<grpc::ServerContext*>(param.server_context),
                     static_cast<RequestType*>(param.request), &rsp);
      });
      static_cast<RequestType*>(param.request)->~RequestType();
    }
    UnaryRunHandlerHelper(param, static_cast<BaseResponseType*>(&rsp), status);
  }

  void* Deserialize(grpc_call* call, grpc_byte_buffer* req,
                    grpc::Status* status, void** /*handler_data*/) final {
    auto* request =
        new (grpc_call_arena_alloc(call, sizeof(RequestType))) RequestType;
    return UnaryDeserializeHelper(req, status,
                                  static_cast<BaseRequestType*>(request));
  }

 private:
  /// Application provided rpc handler function.
  std::function<grpc::Status(ServiceType*, grpc::ServerContext*,
                             const RequestType*, ResponseType*)>
      func_;
  // The class the above handler function lives in.
  ServiceType* service_;
};

/// A wrapper class of an application provided client streaming handler.
template <class ServiceType, class RequestType, class ResponseType>
class ClientStreamingHandler : public grpc::internal::MethodHandler {
 public:
  ClientStreamingHandler(
      std::function<grpc::Status(ServiceType*, grpc::ServerContext*,
                                 ServerReader<RequestType>*, ResponseType*)>
          func,
      ServiceType* service)
      : func_(func), service_(service) {}

  void RunHandler(const HandlerParameter& param) final {
    ServerReader<RequestType> reader(
        param.call, static_cast<grpc::ServerContext*>(param.server_context));
    ResponseType rsp;
    grpc::Status status =
        CatchingFunctionHandler([this, &param, &reader, &rsp] {
          return func_(service_,
                       static_cast<grpc::ServerContext*>(param.server_context),
                       &reader, &rsp);
        });

    grpc::internal::CallOpSet<grpc::internal::CallOpSendInitialMetadata,
                              grpc::internal::CallOpSendMessage,
                              grpc::internal::CallOpServerSendStatus>
        ops;
    if (!param.server_context->sent_initial_metadata_) {
      ops.SendInitialMetadata(&param.server_context->initial_metadata_,
                              param.server_context->initial_metadata_flags());
      if (param.server_context->compression_level_set()) {
        ops.set_compression_level(param.server_context->compression_level());
      }
    }
    if (status.ok()) {
      status =
          ops.SendMessagePtr(&rsp, param.server_context->memory_allocator());
    }
    ops.ServerSendStatus(&param.server_context->trailing_metadata_, status);
    ops.FillOps(param.call);
    param.call->cq()->Pluck(&ops);
  }

 private:
  std::function<grpc::Status(ServiceType*, grpc::ServerContext*,
                             ServerReader<RequestType>*, ResponseType*)>
      func_;
  ServiceType* service_;
};

/// A wrapper class of an application provided server streaming handler.
template <class ServiceType, class RequestType, class ResponseType>
class ServerStreamingHandler : public grpc::internal::MethodHandler {
 public:
  ServerStreamingHandler(std::function<grpc::Status(
                             ServiceType*, grpc::ServerContext*,
                             const RequestType*, ServerWriter<ResponseType>*)>
                             func,
                         ServiceType* service)
      : func_(func), service_(service) {}

  void RunHandler(const HandlerParameter& param) final {
    grpc::Status status = param.status;
    if (status.ok()) {
      ServerWriter<ResponseType> writer(
          param.call, static_cast<grpc::ServerContext*>(param.server_context));
      status = CatchingFunctionHandler([this, &param, &writer] {
        return func_(service_,
                     static_cast<grpc::ServerContext*>(param.server_context),
                     static_cast<RequestType*>(param.request), &writer);
      });
      static_cast<RequestType*>(param.request)->~RequestType();
    }

    grpc::internal::CallOpSet<grpc::internal::CallOpSendInitialMetadata,
                              grpc::internal::CallOpServerSendStatus>
        ops;
    if (!param.server_context->sent_initial_metadata_) {
      ops.SendInitialMetadata(&param.server_context->initial_metadata_,
                              param.server_context->initial_metadata_flags());
      if (param.server_context->compression_level_set()) {
        ops.set_compression_level(param.server_context->compression_level());
      }
    }
    ops.ServerSendStatus(&param.server_context->trailing_metadata_, status);
    ops.FillOps(param.call);
    if (param.server_context->has_pending_ops_) {
      param.call->cq()->Pluck(&param.server_context->pending_ops_);
    }
    param.call->cq()->Pluck(&ops);
  }

  void* Deserialize(grpc_call* call, grpc_byte_buffer* req,
                    grpc::Status* status, void** /*handler_data*/) final {
    grpc::ByteBuffer buf;
    buf.set_buffer(req);
    auto* request =
        new (grpc_call_arena_alloc(call, sizeof(RequestType))) RequestType();
    *status = grpc::Deserialize(&buf, request);
    buf.Release();
    if (status->ok()) {
      return request;
    }
    request->~RequestType();
    return nullptr;
  }

 private:
  std::function<grpc::Status(ServiceType*, grpc::ServerContext*,
                             const RequestType*, ServerWriter<ResponseType>*)>
      func_;
  ServiceType* service_;
};

/// A wrapper class of an application provided bidi-streaming handler.
/// This also applies to server-streamed implementation of a unary method
/// with the additional requirement that such methods must have done a
/// write for status to be ok
/// Since this is used by more than 1 class, the service is not passed in.
/// Instead, it is expected to be an implicitly-captured argument of func
/// (through bind or something along those lines)
template <class Streamer, bool WriteNeeded>
class TemplatedBidiStreamingHandler : public grpc::internal::MethodHandler {
 public:
  explicit TemplatedBidiStreamingHandler(
      std::function<grpc::Status(grpc::ServerContext*, Streamer*)> func)
      : func_(func), write_needed_(WriteNeeded) {}

  void RunHandler(const HandlerParameter& param) final {
    Streamer stream(param.call,
                    static_cast<grpc::ServerContext*>(param.server_context));
    grpc::Status status = CatchingFunctionHandler([this, &param, &stream] {
      return func_(static_cast<grpc::ServerContext*>(param.server_context),
                   &stream);
    });

    grpc::internal::CallOpSet<grpc::internal::CallOpSendInitialMetadata,
                              grpc::internal::CallOpServerSendStatus>
        ops;
    if (!param.server_context->sent_initial_metadata_) {
      ops.SendInitialMetadata(&param.server_context->initial_metadata_,
                              param.server_context->initial_metadata_flags());
      if (param.server_context->compression_level_set()) {
        ops.set_compression_level(param.server_context->compression_level());
      }
      if (write_needed_ && status.ok()) {
        // If we needed a write but never did one, we need to mark the
        // status as a fail
        status = grpc::Status(grpc::StatusCode::INTERNAL,
                              "Service did not provide response message");
      }
    }
    ops.ServerSendStatus(&param.server_context->trailing_metadata_, status);
    ops.FillOps(param.call);
    if (param.server_context->has_pending_ops_) {
      param.call->cq()->Pluck(&param.server_context->pending_ops_);
    }
    param.call->cq()->Pluck(&ops);
  }

 private:
  std::function<grpc::Status(grpc::ServerContext*, Streamer*)> func_;
  const bool write_needed_;
};

template <class ServiceType, class RequestType, class ResponseType>
class BidiStreamingHandler
    : public TemplatedBidiStreamingHandler<
          ServerReaderWriter<ResponseType, RequestType>, false> {
 public:
  BidiStreamingHandler(std::function<grpc::Status(
                           ServiceType*, grpc::ServerContext*,
                           ServerReaderWriter<ResponseType, RequestType>*)>
                           func,
                       ServiceType* service)
      // TODO(vjpai): When gRPC supports C++14, move-capture func in the below
      : TemplatedBidiStreamingHandler<
            ServerReaderWriter<ResponseType, RequestType>, false>(
            [func, service](
                grpc::ServerContext* ctx,
                ServerReaderWriter<ResponseType, RequestType>* streamer) {
              return func(service, ctx, streamer);
            }) {}
};

template <class RequestType, class ResponseType>
class StreamedUnaryHandler
    : public TemplatedBidiStreamingHandler<
          ServerUnaryStreamer<RequestType, ResponseType>, true> {
 public:
  explicit StreamedUnaryHandler(
      std::function<
          grpc::Status(grpc::ServerContext*,
                       ServerUnaryStreamer<RequestType, ResponseType>*)>
          func)
      : TemplatedBidiStreamingHandler<
            ServerUnaryStreamer<RequestType, ResponseType>, true>(
            std::move(func)) {}
};

template <class RequestType, class ResponseType>
class SplitServerStreamingHandler
    : public TemplatedBidiStreamingHandler<
          ServerSplitStreamer<RequestType, ResponseType>, false> {
 public:
  explicit SplitServerStreamingHandler(
      std::function<
          grpc::Status(grpc::ServerContext*,
                       ServerSplitStreamer<RequestType, ResponseType>*)>
          func)
      : TemplatedBidiStreamingHandler<
            ServerSplitStreamer<RequestType, ResponseType>, false>(
            std::move(func)) {}
};

/// General method handler class for errors that prevent real method use
/// e.g., handle unknown method by returning UNIMPLEMENTED error.
template <grpc::StatusCode code>
class ErrorMethodHandler : public grpc::internal::MethodHandler {
 public:
  explicit ErrorMethodHandler(const std::string& message) : message_(message) {}

  template <class T>
  static void FillOps(grpc::ServerContextBase* context,
                      const std::string& message, T* ops) {
    grpc::Status status(code, message);
    if (!context->sent_initial_metadata_) {
      ops->SendInitialMetadata(&context->initial_metadata_,
                               context->initial_metadata_flags());
      if (context->compression_level_set()) {
        ops->set_compression_level(context->compression_level());
      }
      context->sent_initial_metadata_ = true;
    }
    ops->ServerSendStatus(&context->trailing_metadata_, status);
  }

  void RunHandler(const HandlerParameter& param) final {
    grpc::internal::CallOpSet<grpc::internal::CallOpSendInitialMetadata,
                              grpc::internal::CallOpServerSendStatus>
        ops;
    FillOps(param.server_context, message_, &ops);
    ops.FillOps(param.call);
    param.call->cq()->Pluck(&ops);
  }

  void* Deserialize(grpc_call* /*call*/, grpc_byte_buffer* req,
                    grpc::Status* /*status*/, void** /*handler_data*/) final {
    // We have to destroy any request payload
    if (req != nullptr) {
      grpc_byte_buffer_destroy(req);
    }
    return nullptr;
  }

 private:
  const std::string message_;
};

typedef ErrorMethodHandler<grpc::StatusCode::UNIMPLEMENTED>
    UnknownMethodHandler;
typedef ErrorMethodHandler<grpc::StatusCode::RESOURCE_EXHAUSTED>
    ResourceExhaustedHandler;

}  // namespace internal
}  // namespace grpc

#endif  // GRPCPP_SUPPORT_METHOD_HANDLER_H
