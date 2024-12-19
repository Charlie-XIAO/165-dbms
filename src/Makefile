# This file includes automatic dependency tracking. Please see example of utils
# to see how to add additional files to your project

all: client server

# C-compiler settings
CC = gcc -std=c99 -g -ggdb3

# By default use O3 optimization; can be overridden by e.g. `make O=0`
O ?= 3

# Flags and libraries
override CFLAGS += -Wall -Wextra -pedantic -pthread -mavx2 -D_GNU_SOURCE -O$(O) -I$(INCLUDES)
LDFLAGS = -lm
LIBS =
INCLUDES = include

# Additional flags for development build `make DEV=1`
# -save-temps    Save temporary .i and .s files for debugging.
# -fverbose-asm  Extra information in the assembly output.
# -DLOG=1        Enable logging.
ifdef DEV
override CFLAGS += -save-temps -fverbose-asm -DLOG=1
endif

# This is the DB server and client's Unix socket path. Note that the extra
# backslash is for escaping the quotes for Makefile format.
SOCK_PATH = \"/tmp/cs165_unix_socket\"

# This is the path to the data persistence directory. Note that the extra
# backslash is for escaping the quotes for Makefile format.
DB_PERSIST_DIR_PATH = .cs165_db
DB_PERSIST_DIR = \"$(DB_PERSIST_DIR_PATH)\"

###################### Begin AUTOMATIC DEPENDENCY MAGIC ######################

# Set up dependency directory
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP \
	-DSOCK_PATH=$(SOCK_PATH) -DDB_PERSIST_DIR=$(DB_PERSIST_DIR)

# Dependency compilation
ifneq ($(DEP_CC),$(CC) $(CFLAGS) $(DEPCFLAGS) $(O))
DEP_CC := $(shell mkdir -p $(DEPSDIR); \
	echo >$(BUILDSTAMP); \
	echo "DEP_CC:=$(CC) $(CFLAGS) $(DEPCFLAGS) $(O)" >$(DEPSDIR)/_cc.d)
endif

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

###################### End AUTOMATIC DEPENDENCY MAGIC ######################

%.o : %.c $(BUILDSTAMP)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -O$(O) -o $@ -c $<

BINS = client server
UNITTESTBINS = test_binsearch test_bptree test_sort
COMMANDS = addsub agg batch create delete fetch insert join load print select update

client: client.o comm.o io.o logging.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

server: server.o binsearch.o bptree.o cindex.o client_context.o comm.o \
	db_operator.o db_schema.o io.o join.o logging.o parse.o scan.o sort.o \
	sysinfo.o thread_pool.o $(addsuffix .o,$(addprefix cmd,$(COMMANDS)))
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

test_binsearch: test_binsearch.o binsearch.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

test_bptree: test_bptree.o bptree.o binsearch.o sort.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

test_sort: test_sort.o sort.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

unittests: $(UNITTESTBINS)
	@for test in $(UNITTESTBINS); do \
		./$$test; \
	done

clean:
	rm -f *.i *.s *.o *~ *.bak core *.core $(SOCK_PATH) $(BINS) $(UNITTESTBINS)
	rm -rf $(DEPSDIR)

distclean:
	rm -rf $(DB_PERSIST_DIR_PATH)

###################### Begin MIRROR DEV TARGETS FROM ROOT ######################

devprep:
	@$(MAKE) --no-print-directory -C .. devprep

devclean:
	@$(MAKE) --no-print-directory -C .. devclean

devlint:
	@$(MAKE) --no-print-directory -C .. devlint

devreaddb:
	@$(MAKE) --no-print-directory -C .. devreaddb

devgen:
	@$(MAKE) --no-print-directory -C .. devgen

devreport:
	@$(MAKE) --no-print-directory -C .. devreport

devserverreport:
	@$(MAKE) --no-print-directory -C .. devserverreport

devtest:
	@$(MAKE) --no-print-directory -C .. devtest

###################### End MIRROR DEV TARGETS FROM ROOT ######################

.PHONY: all clean distclean
