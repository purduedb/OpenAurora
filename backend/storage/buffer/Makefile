#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/buffer
#
# IDENTIFICATION
#    src/backend/storage/buffer/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	buf_init.o \
	buf_table.o \
	bufmgr.o \
	freelist.o \
	localbuf.o \
	mempool_shmem.o

include $(top_srcdir)/src/backend/common.mk
