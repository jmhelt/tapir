d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(addprefix $(d), waitdie-test.cc woundwait-test.cc)

$(d)waitdie-test: $(o)waitdie-test.o $(LIB-waitdie) $(GTEST_MAIN)

$(d)woundwait-test: $(o)woundwait-test.o $(LIB-woundwait) $(GTEST_MAIN)

TEST_BINS += $(d)waitdie-test $(d)woundwait-test
