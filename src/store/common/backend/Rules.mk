d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
				kvstore.cc lockserver.cc txnstore.cc versionstore.cc messageserver.cc)

LIB-store-backend := $(o)kvstore.o $(o)lockserver.o $(o)txnstore.o $(o)versionstore.o $(o)messageserver.o

include $(d)tests/Rules.mk
