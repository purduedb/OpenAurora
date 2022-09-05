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
	localbuf.o

# POLAR objects
OBJS += polar_copybuf.o polar_flushlist.o polar_xlogbuf.o polar_pbp.o

include $(top_srcdir)/src/backend/common.mk
