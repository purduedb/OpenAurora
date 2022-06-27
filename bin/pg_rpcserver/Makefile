#-------------------------------------------------------------------------
#
# Makefile for src/bin/initdb
#
# Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_rpcserver/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_rpcserver"
PGAPPICON=win32

subdir = src/bin/pg_rpcserver
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) -I$(top_srcdir)/src/timezone $(CPPFLAGS)

# Note: it's important that we link to encnames.o from libpgcommon, not
# from libpq, else we have risks of version skew if we run with a libpq
# shared library from a different PG version.  The libpq_pgport macro
# should ensure that that happens.
#
# We need libpq only because fe_utils does.
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

# use system timezone data?
ifneq (,$(with_system_tzdata))
override CPPFLAGS += '-DSYSTEMTZDIR="$(with_system_tzdata)"'
endif

OBJS = \
	$(WIN32RES) \
	DataPageAccess.o \
	pg_rpcserver.o \

all: pg_rpcserver

pg_rpcserver: $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_rpcserver$(X) '$(DESTDIR)$(bindir)/pg_rpcserver$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_rpcserver$(X)'

clean distclean maintainer-clean:
	rm -f pg_rpcserver$(X) $(OBJS)
	rm -rf tmp_check

# ensure that changes in datadir propagate into object file
pg_rpcserver.o: pg_rpcserver.cpp $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
