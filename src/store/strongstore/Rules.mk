d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), occstore.cc server.cc \
					client.cc shardclient.cc replicaclient.cc \
					strongbufferclient.cc networkconfig.cc waitdie.cc locktable.cc transactionstore.cc)

PROTOS += $(addprefix $(d), strong-proto.proto)

LIB-waitdie := $(o)waitdie.o $(LIB-store-common) $(LIB-message)

OBJS-shard-client := $(LIB-latency) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)shardclient.o

OBJS-replica-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)replicaclient.o

LIB-strong-store := $(o)occstore.o $(o)locktable.o $(o)transactionstore.o $(LIB-waitdie) $(OBJS-shard-client) $(OBJS-replica-client)

OBJS-strong-store := $(LIB-udptransport) $(OBJS-vr-replica) \
    $(LIB-message) $(LIB-strong-store) $(LIB-store-common) \
	$(LIB-store-backend) $(o)strong-proto.o $(o)server.o

OBJS-strong-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)networkconfig.o $(o)strongbufferclient.o $(o)shardclient.o $(o)client.o

include $(d)tests/Rules.mk