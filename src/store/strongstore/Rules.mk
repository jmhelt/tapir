d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), occstore.cc lockstore.cc server.cc \
					client.cc shardclient.cc intershardclient.cc coordinator.cc \
					strongbufferclient.cc)

PROTOS += $(addprefix $(d), strong-proto.proto)

OBJS-intershard-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)shardclient.o $(o)intershardclient.o

LIB-strong-store := $(o)occstore.o $(o)lockstore.o $(o)coordinator.o $(OBJS-intershard-client)

OBJS-strong-store := $(LIB-udptransport) $(OBJS-vr-replica) \
    $(LIB-message) $(LIB-strong-store) $(LIB-store-common) \
	$(LIB-store-backend) $(o)strong-proto.o $(o)server.o

OBJS-strong-client := $(LIB-latency) $(OBJS-vr-client) $(LIB-udptransport) $(LIB-store-frontend) $(LIB-store-common) $(o)strong-proto.o $(o)strongbufferclient.o $(o)shardclient.o $(o)client.o

