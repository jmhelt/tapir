#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

namespace rss {

using barrier_func_t = std::function<void()>;

class Session;

void RegisterRSSService(const std::string &name, barrier_func_t bf);
void UnregisterRSSService(const std::string &name);

void StartRWTransaction(Session &s, const std::string &name);
void EndRWTransaction(Session &s, const std::string &name);

void StartROTransaction(Session &s, const std::string &name);
void EndROTransaction(Session &s, const std::string &name);

class Session {
   public:
    Session();
    ~Session();

   protected:
    Session(const Session &s);

    uint64_t id() const { return id_; }

    void StartRWTransaction(const std::string &name);
    void EndRWTransaction(const std::string &name);

    void StartROTransaction(const std::string &name);
    void EndROTransaction(const std::string &name);

    friend void StartRWTransaction(Session &s, const std::string &name);
    friend void EndRWTransaction(Session &s, const std::string &name);
    friend void StartROTransaction(Session &s, const std::string &name);
    friend void EndROTransaction(Session &s, const std::string &name);

   private:
    enum RSSServiceState {
        NONE = 0,
        EXECUTING_RW,
        EXECUTING_RO,
        EXECUTED_RW,
        EXECUTED_RO
    };

    void UpdateLastService(const std::string &name);

    static std::atomic<std::uint64_t> next_id_;

    uint64_t id_;
    std::string last_service_;
    RSSServiceState current_state_;
};

class RSSRegistry {
   public:
    RSSRegistry();
    ~RSSRegistry();

   protected:
    class RSSService {
       public:
        RSSService(std::string name, barrier_func_t bf);
        ~RSSService();

        void invoke_barrier() const { bf_(); }

       private:
        std::string name_;
        barrier_func_t bf_;
    };

    void RegisterRSSService(const std::string &name, barrier_func_t bf);
    void UnregisterRSSService(const std::string &name);

    RSSService &FindService(const std::string &name);

    friend Session;

    friend void RegisterRSSService(const std::string &name, barrier_func_t bf);
    friend void UnregisterRSSService(const std::string &name);

   private:
    std::unordered_map<std::string, RSSService> services_;
};

}  // namespace rss