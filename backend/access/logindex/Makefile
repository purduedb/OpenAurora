#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/index
#
# IDENTIFICATION
#    src/backend/access/index/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/logindex
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	polar_logindex.o \
	brin_xlog_idx.o \
	generic_xlog_idx.o \
	gin_xlog_idx.o \
	gist_xlog_idx.o \
	hash_xlog_idx.o \
	heap_xlog_idx.o \
	nbtxlog_idx.o \
	polar_sequence_xlog_idx.o \
	polar_xlog_idx.o \
	spgxlog_idx.o \
	logindex_hashmap.o \
	logindex_func.o \
	background_hashmap_vacuumer.o \
	wakeup_latch.o

include $(top_srcdir)/src/backend/common.mk
