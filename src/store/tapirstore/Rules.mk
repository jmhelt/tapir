d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc \
	server.cc store.cc)

PROTOS += $(addprefix $(d), tapir-proto.proto)

OBJS-tapir-store := $(OBJS-ir-replica) $(LIB-message) $(LIB-store-common) $(LIB-store-backend) \
	$(o)tapir-proto.o $(o)store.o $(o)server.o

OBJS-tapir-client := $(OBJS-ir-client)  $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)tapir-proto.o \
		$(o)shardclient.o $(o)client.o
