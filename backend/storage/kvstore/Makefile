
#INCLUDES =  -L.
#LINKLIB = -lrocksdb

#main: main.o
#	g++ main.c -o main $(INCLUDES) $(LINKLIB) -O2 -std=c++17 -lrt -ldl -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd  -ldl -lpthread

#main.o: main.c
#	cc -c $(INCLUDES) main.c


subdir = src/backend/storage/kvstore
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	kv_interface.o
#	lru_node.o \
#	concurrent_hashmap.o \
#	concurrent_hashmap_bucket.o \
#	concurrent_hashmap_node.o \
#	concurrent_lru_buffer_pool.o \
#	light_weighted_kvstore.o \
#	disk_manager.o \

include $(top_srcdir)/src/backend/common.mk