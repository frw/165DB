# CS165 MakeFile
# This file includes automatic dependency tracking
# Please see example of "utils" to see how to add additional file
# to your project

all: client server

debug: CC += -g -ggdb3 -D LOG -D LOG_ERR -D LOG_INFO
debug: O = 0
debug: all

# C-compiler settings
CC = gcc -std=c99

# Default optimization level
O ?= fast

# Flags and other libraries
override CFLAGS += -Wall -Wextra -Werror -pedantic -fshort-enums -pthread -O$(O) -I$(INCLUDES)
LDFLAGS =
LIBS = -lm
INCLUDES = include

####### Automatic dependency magic #######
# Set-up dependency directory
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP

# Dependency compilation
ifneq ($(DEP_CC),$(CC) $(CFLAGS) $(DEPCFLAGS) $(O))
DEP_CC := $(shell mkdir -p $(DEPSDIR); echo >$(BUILDSTAMP); echo "DEP_CC:=$(CC) $(CFLAGS) $(DEPCFLAGS) $(O)" >$(DEPSDIR)/_cc.d)
endif

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

####### Automatic dependency magic #######

%.o : %.c $(BUILDSTAMP)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -O$(O) -o $@ -c $<

##
# To include additional non-executable files (e.g. selects.c, utils.c, etc),
# you'll need to add an additional build dependency to the file that requires
# the new file.  For example, see that client and server both require utils.o
#
# If you create a new file such as selects.c, then you will need a "selects.o"
# dependency on the right side of whichever one requires the file.
##

client: client.o common.o hash_table.o utils.o vector.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

server: client_context.o common.o db_manager.o db_operator.o dsl.o hash_table.o parser.o server.o utils.o vector.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f client server *.o *~ *.bak core *.core cs165_unix_socket
	rm -rf $(DEPSDIR)

distclean: clean
	rm -rf data

.PHONY: all debug clean distclean
