#pragma once

#include <functional>
#include <string>
#include <unordered_map>

namespace rss {

using barrier_func_t = std::function<void()>;

void RegisterRSSService(const std::string &name, barrier_func_t bf);
void UnregisterRSSService(const std::string &name);

void StartRWTransaction(const std::string &name);
void EndRWTransaction(const std::string &name);

void StartROTransaction(const std::string &name);
void EndROTransaction(const std::string &name);

class RSSRegistry {
   public:
    RSSRegistry();
    ~RSSRegistry();

   protected:
    void RegisterRSSService(const std::string &name, barrier_func_t bf);
    void UnregisterRSSService(const std::string &name);

    void StartRWTransaction(const std::string &name);
    void EndRWTransaction(const std::string &name);

    void StartROTransaction(const std::string &name);
    void EndROTransaction(const std::string &name);

    friend void RegisterRSSService(const std::string &name, barrier_func_t bf);
    friend void UnregisterRSSService(const std::string &name);
    friend void StartRWTransaction(const std::string &name);
    friend void EndRWTransaction(const std::string &name);
    friend void StartROTransaction(const std::string &name);
    friend void EndROTransaction(const std::string &name);

   private:
    enum RSSServiceState {
        NONE,
        EXECUTING_RW,
        EXECUTING_RO
    };

    class RSSService {
       public:
        RSSService(std::string name, barrier_func_t bf);
        ~RSSService();

        RSSServiceState current_state() const { return current_state_; }
        void set_current_state(RSSServiceState s) { current_state_ = s; }

       private:
        std::string name_;
        barrier_func_t bf_;
        RSSServiceState current_state_;
    };

    RSSService &FindService(const std::string &name);

    std::string last_service_;
    std::unordered_map<std::string, RSSService> services_;
};

RSSRegistry registry;

}  // namespace rss