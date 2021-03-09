d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	    vr-proto.proto)

OBJS-vr-client := $(o)client.o $(o)vr-proto.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-latency) \
                   $(LIB-configuration)

OBJS-vr-replica := $(o)replica.o $(o)vr-proto.o \
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-latency) \
                   $(LIB-configuration)

include $(d)tests/Rules.mk

