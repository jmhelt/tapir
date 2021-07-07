
#include <rss/lib.h>

#include <stdexcept>

namespace rss {

void RegisterRSSService(const std::string &name, barrier_func_t bf) {
    registry.RegisterRSSService(name, bf);
}
void UnregisterRSSService(const std::string &name) {
    registry.UnregisterRSSService(name);
}

void StartRWTransaction(const std::string &name) {
    registry.StartRWTransaction(name);
}

void EndRWTransaction(const std::string &name) {
    registry.EndRWTransaction(name);
}

void StartROTransaction(const std::string &name) {
    registry.StartROTransaction(name);
}

void EndROTransaction(const std::string &name) {
    registry.EndROTransaction(name);
}

RSSRegistry::RSSService::RSSService(std::string name, barrier_func_t bf)
    : name_{name}, bf_{bf}, current_state_{NONE} {}

RSSRegistry::RSSService::~RSSService() {}

RSSRegistry::RSSRegistry() : last_service_{""} {}

RSSRegistry::~RSSRegistry() {}

void RSSRegistry::RegisterRSSService(const std::string &name, barrier_func_t bf) {
    auto search = services_.find(name);
    if (search != services_.end()) {
        throw new std::runtime_error("Duplicate service registration: " + name);
    }

    services_.insert({name, {name, bf}});
}

void RSSRegistry::UnregisterRSSService(const std::string &name) {
    auto search = services_.find(name);
    if (search == services_.end()) {
        throw new std::runtime_error("Service not found: " + name);
    }

    services_.erase(search);
}

RSSRegistry::RSSService &RSSRegistry::FindService(const std::string &name) {
    auto search = services_.find(name);
    if (search == services_.end()) {
        throw new std::runtime_error("Service not found: " + name);
    }

    return search->second;
}

void RSSRegistry::StartRWTransaction(const std::string &name) {
    auto &service = FindService(name);
    // service.StartRW();
}

void RSSRegistry::EndRWTransaction(const std::string &name) {
    auto &service = FindService(name);
    // service.EndRW();
}

void RSSRegistry::StartROTransaction(const std::string &name) {
    auto &service = FindService(name);
    // service.StartRO();
}

void RSSRegistry::EndROTransaction(const std::string &name) {
    auto &service = FindService(name);
    // service.EndRO();
}

}  // namespace rss