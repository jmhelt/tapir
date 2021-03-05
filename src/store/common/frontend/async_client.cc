#include "store/common/frontend/async_client.h"

void AsyncClient::StartRecLatency() {
  //::Latency_Start(&clientLat);
}

void AsyncClient::EndRecLatency(const std::string &str) {
  //uint64_t ns = ::Latency_End(&clientLat);
  //Notice("%s %lu ns", str.c_str(), ns);
}
