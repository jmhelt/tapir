
#include <rss/lib.h>

#include <stdexcept>

namespace rss {

void RegisterRSSService(const std::string &name, barrier_func_t bf) {
    __RSS_REGISTRY.RegisterRSSService(name, bf);
}
void UnregisterRSSService(const std::string &name) {
    __RSS_REGISTRY.UnregisterRSSService(name);
}

void StartRWTransaction(const std::string &name) {
    __RSS_REGISTRY.StartRWTransaction(name);
}

void EndRWTransaction(const std::string &name) {
    __RSS_REGISTRY.EndRWTransaction(name);
}

void StartROTransaction(const std::string &name) {
    __RSS_REGISTRY.StartROTransaction(name);
}

void EndROTransaction(const std::string &name) {
    __RSS_REGISTRY.EndROTransaction(name);
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

void RSSRegistry::UpdateLastService(const std::string &name) {
    if (!last_service_.empty() && last_service_ != name) {
        auto last = FindService(last_service_);

        auto state = last.current_state();
        switch (state) {
            case EXECUTED_RO:
                last.invoke_barrier();
                break;
            case NONE:
            case EXECUTED_RW:
                break;
            case EXECUTING_RW:
            case EXECUTING_RO:
                throw new std::runtime_error("Invalid state transition: Still executing transaction at previous service");
        }

        last_service_ = name;
    }
}

void RSSRegistry::StartRWTransaction(const std::string &name) {
    auto &service = FindService(name);

    auto state = service.current_state();
    switch (state) {
        case NONE:
        case EXECUTED_RW:
        case EXECUTED_RO:
            service.set_current_state(EXECUTING_RW);
            break;
        case EXECUTING_RW:
        case EXECUTING_RO:
            throw new std::runtime_error("Invalid state transition: Already executing transaction");
    }

    UpdateLastService(name);
}

void RSSRegistry::EndRWTransaction(const std::string &name) {
    auto &service = FindService(name);

    auto state = service.current_state();
    switch (state) {
        case EXECUTING_RW:
            service.set_current_state(EXECUTED_RW);
            break;
        case NONE:
        case EXECUTING_RO:
        case EXECUTED_RW:
        case EXECUTED_RO:
            throw new std::runtime_error("Invalid state transition: Not executing RW transaction");
    }
}

void RSSRegistry::StartROTransaction(const std::string &name) {
    auto &service = FindService(name);

    auto state = service.current_state();
    switch (state) {
        case NONE:
        case EXECUTED_RW:
        case EXECUTED_RO:
            service.set_current_state(EXECUTING_RO);
            break;
        case EXECUTING_RW:
        case EXECUTING_RO:
            throw new std::runtime_error("Invalid state transition: Already executing transaction");
    }

    UpdateLastService(name);
}

void RSSRegistry::EndROTransaction(const std::string &name) {
    auto &service = FindService(name);

    auto state = service.current_state();
    switch (state) {
        case EXECUTING_RO:
            service.set_current_state(EXECUTED_RO);
            break;
        case NONE:
        case EXECUTING_RW:
        case EXECUTED_RW:
        case EXECUTED_RO:
            throw new std::runtime_error("Invalid state transition: Not executing RW transaction");
    }
}

}  // namespace rss