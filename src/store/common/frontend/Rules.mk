d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), async_adapter_client.cc \
						  async_client.cc \
						  async_transaction.cc \
						  bufferclient.cc \
						  sync_client.cc \
						  transaction_utils.cc)

LIB-store-frontend := $(LIB-store-common) \
					  $(o)async_adapter_client.o \
					  $(o)async_client.o \
					  $(o)async_transaction.o \
					  $(o)bufferclient.o \
					  $(o)sync_client.o \
					  $(o)transaction_utils.o
