#include "store/common/frontend/transaction_utils.h"

Operation Wait() {
    return Operation{WAIT, "", ""};
}

Operation Get(const std::string &key) {
    return Operation{GET, key, ""};
}

Operation GetForUpdate(const std::string &key) {
    return Operation{GET_FOR_UPDATE, key, ""};
}

Operation Put(const std::string &key,
              const std::string &value) {
    return Operation{PUT, key, value};
}

Operation Commit() {
    return Operation{COMMIT, "", ""};
}

Operation Abort() {
    return Operation{ABORT, "", ""};
}

Operation ROCommit(const std::unordered_set<std::string> &keys) {
    return Operation{ROCOMMIT, "", "", keys};
}

Operation ROCommit(const std::unordered_set<std::string> &&keys) {
    return Operation{ROCOMMIT, "", "", keys};
}
