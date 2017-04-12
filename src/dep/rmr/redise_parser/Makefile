
RM_INCLUDE_DIR ?= ../../../
SOURCEDIR=$(shell pwd -P)
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) $(wildcard $(SOURCEDIR)/dep/*.c) 
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))
CFLAGS = -I"$(RM_INCLUDE_DIR)" -Wall $(DEBUGFLAGS) -fPIC -std=gnu99  
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')


libparser.a: generate | $(CC_OBJECTS)
	ar rcs $@ $(CC_OBJECTS)

generate:
	lex -i lexer.l
	lemon -s grammar.y

.PHONY: libparser.a
all: libparser.a