#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/hash
#
# IDENTIFICATION
#    src/backend/access/hash/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/logindex
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = polar_logindex.o \
	   polar_logindex_iterator.o \
	   polar_mini_transaction.o \
	   polar_ringbuf.o \
	   polar_queue_manager.o \
	   polar_rel_size_cache.o \
	   polar_logindex_redo.o \
	   polar_storage_idx.o \
	   polar_xlog_idx.o \
	   polar_heap_idx.o \
	   polar_nbtxlog_idx.o \
	   polar_hash_xlog_idx.o \
	   polar_ginxlog_idx.o \
	   polar_gistxlog_idx.o \
	   polar_sequence_xlog_idx.o \
	   polar_spgxlog_idx.o \
	   polar_brin_xlog_idx.o \
	   polar_generic_xlog_idx.o \
	   polar_fullpage.o \
	   polar_logindex_bg_worker.o

include $(top_srcdir)/src/backend/common.mk
