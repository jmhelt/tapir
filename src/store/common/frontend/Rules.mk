d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), async_transaction.cc \
						  bufferclient.cc \
						  sync_client.cc \
						  transaction_utils.cc)

LIB-store-frontend := $(LIB-store-common) \
					  $(o)async_transaction.o \
					  $(o)bufferclient.o \
					  $(o)sync_client.o \
					  $(o)transaction_utils.o
