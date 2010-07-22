.POSIX:

INSTALL ?= install
ASCIIDOC ?= asciidoc
SOURCES := subprocess.c
VERSION := 0.01
DISTDIR := subprocess-$(VERSION)
DISTFILES := Makefile $(SOURCES) subprocess.txt subprocess.html

INSTALL_CMOD := $(shell pkg-config --variable=INSTALL_CMOD lua)

ifeq ($(INSTALL_CMOD),)
$(error Lua package not found)
endif

CFLAGS ?= -Wall -Wextra -pedantic -O2
LUA_CFLAGS := $(shell pkg-config --cflags --libs lua)

.PHONY: all
all: subprocess.so subprocess.html

subprocess.so: $(SOURCES)
	$(CC) $(CFLAGS) $(LUA_CFLAGS) -shared -fPIC -o $@ $(SOURCES)

subprocess.html: subprocess.txt
	$(ASCIIDOC) $<

.PHONY: clean
clean:
	$(RM) subprocess.so

.PHONY: install
install: subprocess.so
	$(INSTALL) -m755 subprocess.so $(INSTALL_CMOD)/

.PHONY: uninstall
uninstall:
	$(RM) $(INSTALL_CMOD)/subprocess.so

.PHONY: dist
dist: $(DISTFILES)
	[ -d $(DISTDIR) ] || mkdir $(DISTDIR)
	$(INSTALL) -m644 $(DISTFILES) $(DISTDIR)/
	tar -czf $(DISTDIR).tar.gz $(DISTDIR)
