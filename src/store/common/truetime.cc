// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.cc:
 *   A simulated TrueTime module
 *
 **********************************************************************/

#include "store/common/truetime.h"

#include <chrono>

TrueTime::TrueTime() : error_{0} {}

TrueTime::TrueTime(uint64_t error) : error_{error} {
    Debug("TrueTime variance: error=%lu", error_);
}

uint64_t TrueTime::GetTime() const {
    auto now = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::microseconds>(
                     now.time_since_epoch())
                     .count();

    return static_cast<uint64_t>(count);
}

TrueTimeInterval TrueTime::Now() const {
    uint64_t time = GetTime();
    Debug("Now: %lu", error_);
    return {time - error_, time + error_};
}
