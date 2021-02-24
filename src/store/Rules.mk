d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc)

$(d)server: $(OBJS-tapir-store) $(OBJS-strong-store) $(OBJS-weak-store) \
	$(LIB-udptransport) $(LIB-tcptransport) $(LIB-io-utils) $(LIB-store-common) $(o)server.o 

BINS += $(d)server
