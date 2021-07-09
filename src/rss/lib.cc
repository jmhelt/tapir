
#include <rss/lib.h>

#include <iostream>
#include <stdexcept>

namespace rss {

static RSSRegistry RSS_REGISTRY;

void RegisterRSSService(const std::string &name, barrier_func_t bf) {
    RSS_REGISTRY.RegisterRSSService(name, bf);
}
void UnregisterRSSService(const std::string &name) {
    RSS_REGISTRY.UnregisterRSSService(name);
}

void StartRWTransaction(Session &s, const std::string &name) {
    s.StartRWTransaction(name);
}

void EndRWTransaction(Session &s, const std::string &name) {
    s.EndRWTransaction(name);
}

void StartROTransaction(Session &s, const std::string &name) {
    s.StartROTransaction(name);
}

void EndROTransaction(Session &s, const std::string &name) {
    s.EndROTransaction(name);
}

std::atomic<std::uint64_t> Session::next_id_{0};

Session::Session() : id_{next_id_++}, last_service_{""}, current_state_{NONE} {
    std::cerr << "Session created: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

Session::Session(const Session &s)
    : id_{s.id_}, last_service_{s.last_service_}, current_state_{s.current_state_} {
    std::cerr << "Session continued: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

Session::~Session() {
    std::cerr << "Session destroyed: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

void Session::UpdateLastService(const std::string &name) {
    if (!last_service_.empty() && last_service_ != name) {
        auto last = RSS_REGISTRY.FindService(last_service_);

        switch (current_state_) {
            case EXECUTED_RO:
                last.invoke_barrier();
                break;
            case NONE:
            case EXECUTED_RW:
                break;
            case EXECUTING_RW:
            case EXECUTING_RO:
                std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
                std::cerr << "Invalid state transition: Still executing transaction at previous service" << std::endl;
                throw new std::runtime_error("Invalid state transition: Still executing transaction at previous service");
            default:
                throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
        }
    }

    last_service_ = name;
}

void Session::StartRWTransaction(const std::string &name) {
    switch (current_state_) {
        case NONE:
        case EXECUTED_RW:
        case EXECUTED_RO:
            current_state_ = EXECUTING_RW;
            break;
        case EXECUTING_RW:
        case EXECUTING_RO:
            std::cerr << "Invalid state transition: Already executing transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Already executing transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }

    UpdateLastService(name);
}

void Session::EndRWTransaction(const std::string &name) {
    switch (current_state_) {
        case EXECUTING_RW:
            current_state_ = EXECUTED_RW;
            break;
        case NONE:
        case EXECUTING_RO:
        case EXECUTED_RW:
        case EXECUTED_RO:
            std::cerr << "Invalid state transition: Not executing RW transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Not executing RW transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }
}

void Session::StartROTransaction(const std::string &name) {
    switch (current_state_) {
        case NONE:
        case EXECUTED_RW:
        case EXECUTED_RO:
            current_state_ = EXECUTING_RO;
            break;
        case EXECUTING_RW:
        case EXECUTING_RO:
            std::cerr << "Invalid state transition: Already executing transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Already executing transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }

    UpdateLastService(name);
}

void Session::EndROTransaction(const std::string &name) {
    switch (current_state_) {
        case EXECUTING_RO:
            current_state_ = EXECUTED_RO;
            break;
        case NONE:
        case EXECUTING_RW:
        case EXECUTED_RW:
        case EXECUTED_RO:
            std::cerr << "Invalid state transition: Not executing RO transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Not executing RO transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }
}

RSSRegistry::RSSService::RSSService(std::string name, barrier_func_t bf)
    : name_{name}, bf_{bf} {}

RSSRegistry::RSSService::~RSSService() {}

RSSRegistry::RSSRegistry() : services_{} {}

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
        std::cerr << "Service not found: " << name << std::endl;
        throw new std::runtime_error("Service not found: " + name);
    }

    return search->second;
}

}  // namespace rss