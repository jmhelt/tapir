d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), bench_client.cc \
						  benchmark.cc \
						  open_bench_client.cc \
						  sync_transaction_bench_client.cc)

OBJS-all-store-clients := $(OBJS-strong-client) $(OBJS-weak-client) $(OBJS-tapir-client)

LIB-bench-client := $(o)bench_client.o \
					$(o)benchmark.o \
					$(o)open_bench_client.o \
					$(o)sync_transaction_bench_client.o

OBJS-all-bench-clients := $(LIB-retwis)

$(d)benchmark: $(LIB-key-selector) $(LIB-bench-client) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(OBJS-all-store-clients) $(OBJS-all-bench-clients) $(LIB-bench-client) $(LIB-store-common)

BINS +=  $(d)benchmark
