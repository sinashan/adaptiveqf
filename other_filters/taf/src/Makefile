#compiler
CC=gcc
#CC = clang	

#put any desired compilation flags here.  Feel free to remove O2 or change to O3 or Ofast	
#make sure to run "make clean" if this is changed	
ifndef D
	CFLAGS=-O3 -lm
	#SPLINTERPATH=../../../external/splinterdb/build/release/lib
	SPLINTERPATH=../../../external/splinterdb/btree
else
	CFLAGS=-g -O0 -lm -rdynamic -Wall
	#SPLINTERPATH=../../../external/splinterdb/build/debug/lib
	SPLINTERPATH=../../../external/splinterdb/btree
endif

#flags to use when running gprof
PROFFLAGS=-pg -O0 -lm

#operating system (for Max)
OS := $(shell uname)
ifeq ($(OS), Darwin)
CFLAGS += -Wno-unused-command-line-argument -Wformat=0
PROFFLAGS += -Wno-unused-command-line-argument -Wformat=0
else
endif

DEPS = arcd.h constants.h macros.h murmur3.h bit_util.h remainder.h rsqf.h set.h
OBJ = arcd.o exaf.o murmur3.o bit_util.o rsqf.o set.o

#only need test.out to build 'all' of project
all: test.out	

#adds flags (set above) for make debug
#make sure to run "make clean" if no changes to source files	
debug: CFLAGS=$(DEBUGFLAGS)
debug: test.out

#profile with gprof
prof: CFLAGS=$(PROFFLAGS)
prof: test.out

.PHONY: all clean

rsqf: rsqf.c
	$(CC) -D TEST_RSQF=1 -o rsqf.o rsqf.c murmur3.c bit_util.c set.c $(CFLAGS)

exaf: exaf.c
	$(CC) -D TEST_EXAF=1 -o exaf.o exaf.c arcd.c murmur3.c bit_util.c set.c $(CFLAGS)

utaf: utaf.c
	$(CC) -D TEST_UTAF=1 -o utaf.o utaf.c arcd.c murmur3.c bit_util.c set.c $(CFLAGS)

taf: taf.c
	$(CC) -D TEST_TAF=1 -o taf.o taf.c arcd.c murmur3.c bit_util.c set.c $(CFLAGS) -lssl -lcrypto

taf_sim: taf_sim.c
	$(CC) -D TEST_TAF_SIM=1 -o taf_sim.o taf_sim.c arcd.c murmur3.c bit_util.c set.c $(CFLAGS) -lssl -lcrypto -lsplinterdb -DSPLINTERDB_PLATFORM_DIR=platform_linux -D_GNU_SOURCE

splinter_taf: splinter_taf.c
	$(CC) -D TEST_SPLINTER_TAF=1 -o splinter_taf.o splinter_taf.c arcd.c murmur3.c bit_util.c set.c -I../../../external/splinterdb/include $(CFLAGS) -lssl -lcrypto -L$(SPLINTERPATH) -lsplinterdb -DSPLINTERDB_PLATFORM_DIR=platform_linux -D_GNU_SOURCE -Wl,-rpath=$(SPLINTERPATH)

arcd: arcd.c
	$(CC) -D TEST_ARCD=1 -o arcd.o arcd.c $(CFLAGS)

# $@ = target name
# $^ = all prereqs

# this rule says that every .o file needs to be compiled using the corresponding .c file
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

#this rule links the object files together and stores the output in test.out	
test.out: $(OBJ)	
	$(CC) -o $@ $^ $(CFLAGS)	

#a possibly-sloppy way to undo making: remove all object files
clean: 	
	rm $(OBJ)
