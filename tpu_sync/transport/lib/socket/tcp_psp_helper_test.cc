// Copyright 2026 Google LLC.
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

#include "tpu_sync/transport/lib/socket/tcp_psp_helper.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "grpcpp/channel.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/support/channel_arguments.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "tpu_sync/transport/lib/peregrine_control_service.h"
#include "tpu_sync/transport/lib/raw_buffer_transport.h"
#include "tpu_sync/transport/lib/raw_buffer_transport_delegate.h"
#include "tpu_sync/transport/lib/socket/psp_syscall_mock.h" // NOLINT
#include "tpu_sync/transport/lib/socket/util.h"

namespace tpu_raiden::transport::lib {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Field;
using ::testing::Ne;
using ::testing::NotNull;

constexpr absl::string_view kValidKey("0123456789\0\0\0\0\0", 16); // NOLINT

class FakeRawDelegate : public RawBufferTransportDelegate {
 public:
  uint8_t* GetHostPointer(size_t buffer_id, size_t shard_idx) override {
    return nullptr;
  }
  size_t GetHostSize(size_t buffer_id, size_t shard_idx) override { return 0; }
};

TEST(TcpPspHelperTest, RegisterPspKey) {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(server_fd, 0);

  EXPECT_THAT(RegisterPspPeerKey(server_fd, 0x12345678, kValidKey),
              IsOkAndHolds(Field(&PspPeerKey::spi, Ne(0))));
  EXPECT_THAT(RegisterPspPeerKey(server_fd, 0, kValidKey),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(RegisterPspPeerKey(server_fd, 0x12345678, "short"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  ::close(server_fd);
}

TEST(TcpPspHelperTest, PspEnabledVerification) {
  int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock_fd, 0);

  EXPECT_TRUE(PspEnabled(sock_fd));
  EXPECT_FALSE(PspEnabled(-1));

  ::close(sock_fd);
}

TEST(TcpPspHelperTest, EndToEndPspKeyExchangeAndConnect) {
  FakeRawDelegate raw_delegate;
  RawBufferTransport transport(&raw_delegate, /*local_port=*/0);
  PeregrineControlServiceImpl service(&transport);

  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_THAT(server, NotNull());

  std::shared_ptr<grpc::Channel> channel =
      server->InProcessChannel(grpc::ChannelArguments());

  std::string peer = absl::StrCat("127.0.0.1:", transport.local_port());
  auto client_fd_or = ConnectToPeer(peer, "127.0.0.1", channel);
  ASSERT_TRUE(client_fd_or.ok()) << client_fd_or.status();
  int client_fd = *client_fd_or;
  EXPECT_GE(client_fd, 0);

  ::close(client_fd);
  server->Shutdown();
}

}  // namespace
}  // namespace tpu_raiden::transport::lib
