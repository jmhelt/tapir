d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), occstore.cc lockstore.cc server.cc \
					client.cc shardclient.cc replicaclient.cc coordinator.cc \
					strongbufferclient.cc networkconfig.cc)

PROTOS += $(addprefix $(d), strong-proto.proto)

OBJS-shard-client := $(LIB-latency) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)shardclient.o

OBJS-replica-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)replicaclient.o

LIB-strong-store := $(o)occstore.o $(o)lockstore.o $(o)coordinator.o $(OBJS-shard-client) $(OBJS-replica-client)

OBJS-strong-store := $(LIB-udptransport) $(OBJS-vr-replica) \
    $(LIB-message) $(LIB-strong-store) $(LIB-store-common) \
	$(LIB-store-backend) $(o)strong-proto.o $(o)server.o

OBJS-strong-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)networkconfig.o $(o)strongbufferclient.o $(o)shardclient.o $(o)client.o

