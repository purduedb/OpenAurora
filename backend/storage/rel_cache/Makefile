
#INCLUDES =  -L.
#LINKLIB = -lrocksdb

#main: main.o
#	g++ main.c -o main $(INCLUDES) $(LINKLIB) -O2 -std=c++17 -lrt -ldl -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd  -ldl -lpthread

#main.o: main.c
#	cc -c $(INCLUDES) main.c


subdir = src/backend/storage/rel_cache
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	boost_shmht.o \
	builtin_shmht.o \
	rel_cache.o

include $(top_srcdir)/src/backend/common.mk