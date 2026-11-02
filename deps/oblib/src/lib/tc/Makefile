cxx ?= /usr/local/ob-clang11/bin/clang++
cflags = -g -O2 -Wall -Wno-invalid-offsetof -fno-strict-aliasing -D_GNU_SOURCE #-fsanitize=address -fno-omit-frame-pointer
src-list = $(wildcard deps/*.h *.h)
test-list= test-fallback test-perf test-weight test-weight-L2 test-limit-1leaf test-limit-2leaf test-limit-4leaf test-limit-top test-2limit test-plimit test-plimit2 test-reserve-1leaf test-reserve-2leaf test-reserve-4leaf test-ob test-reserve-clog-2leaf test-reserve-clog-4leaf test-over-reserve-2leaf
all: $(test-list)
test-%: test/test-%.cpp test/test-base.cpp ob_tc.cpp $(src-list)
		$(cxx) -o $@ test/test-base.cpp -DSRC=$<  $(cflags) -lpthread -I.