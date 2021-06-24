d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), occstore.cc server.cc \
					client.cc shardclient.cc replicaclient.cc \
					networkconfig.cc \
					waitdie.cc woundwait.cc locktable.cc \
					preparedtransaction.cc viewfinder.cc transactionstore.cc)

PROTOS += $(addprefix $(d), strong-proto.proto)

LIB-waitdie := $(o)waitdie.o $(LIB-store-common) $(LIB-message)

LIB-woundwait := $(o)woundwait.o $(LIB-store-common) $(LIB-message)

OBJS-shard-client := $(LIB-latency) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)shardclient.o

OBJS-replica-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)replicaclient.o

LIB-strong-store := $(o)occstore.o $(o)locktable.o $(o)transactionstore.o $(LIB-woundwait) $(OBJS-shard-client) $(OBJS-replica-client)

OBJS-strong-store := $(LIB-udptransport) $(OBJS-vr-replica) \
    $(LIB-message) $(LIB-strong-store) $(LIB-store-common) \
	$(LIB-store-backend) $(o)strong-proto.o $(o)preparedtransaction.o $(o)server.o

OBJS-strong-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) \
	$(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)preparedtransaction.o $(o)viewfinder.o \
	$(o)networkconfig.o $(o)shardclient.o $(o)client.o

include $(d)tests/Rules.mk