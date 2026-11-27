CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Werror -pedantic

test: build/test_mark build/emit
	./build/test_mark

build/test_mark: test_mark.c hopwake_mark.c hopwake_mark.h | build
	$(CC) $(CFLAGS) -o $@ test_mark.c hopwake_mark.c

build/emit: tools/emit.c hopwake_mark.c hopwake_mark.h | build
	$(CC) $(CFLAGS) -o $@ tools/emit.c hopwake_mark.c

build:
	mkdir -p build

clean:
	rm -rf build

.PHONY: test clean
