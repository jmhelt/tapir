d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), bufferclient.cc transaction_utils.cc sync_client.cc client.cc)

LIB-store-frontend := $(LIB-store-common) $(o)bufferclient.o \
		$(o)transaction_utils.o $(o)sync_client.o
