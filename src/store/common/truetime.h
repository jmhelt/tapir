// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.h
 *   A simulated TrueTime module
 *
 **********************************************************************/

#ifndef _TRUETIME_H_
#define _TRUETIME_H_

#include "lib/message.h"

class TrueTimeInterval {
   public:
    TrueTimeInterval(uint64_t earliest, uint64_t latest)
        : earliest_{earliest}, latest_{latest} {}

    ~TrueTimeInterval() {}

    uint64_t earliest() { return earliest_; }
    uint64_t latest() { return latest_; }

   private:
    uint64_t earliest_;
    uint64_t latest_;
};

class TrueTime {
   public:
    TrueTime();
    TrueTime(uint64_t error);
    ~TrueTime(){};

    uint64_t GetTime();

    TrueTimeInterval Now();

   private:
    uint64_t error_;
};

#endif /* _TRUETIME_H_ */
