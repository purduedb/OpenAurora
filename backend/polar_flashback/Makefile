#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for flashback
#
# IDENTIFICATION
#    src/backend/polar_flashback/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/polar_flashback
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = polar_flashback_log.o polar_flashback_log_file.o  polar_flashback_log_index.o  polar_flashback_log_mem.o \
 polar_flashback_log_reader.o polar_flashback_log_decoder.o polar_flashback_log_insert.o polar_flashback_log_worker.o \
 polar_flashback_point.o polar_flashback_log_list.o  polar_flashback_log_index_queue.o polar_flashback_log_repair_page.o polar_flashback_drop.o

include $(top_srcdir)/src/backend/common.mk
