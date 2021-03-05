d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), partitioner.cc \
						  pinginitiator.cc \
						  promise.cc \
						  stats.cc \
						  timestamp.cc \
						  tracer.cc \
						  transaction.cc \
						  truetime.cc)

PROTOS += $(addprefix $(d), common-proto.proto)

LIB-store-common := $(o)common-proto.o \
					$(o)partitioner.o \
					$(o)pinginitiator.o \
					$(o)promise.o \
					$(o)stats.o \
					$(o)timestamp.o \
					$(o)tracer.o \
					$(o)transaction.o \
					$(o)truetime.o \


include $(d)backend/Rules.mk $(d)frontend/Rules.mk
