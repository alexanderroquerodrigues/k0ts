CC = cc
CFLAGS = -Wall -Wextra -g -O0
SRC = src/main.c src/state.c src/dedup.c
BIN = auction

$(BIN): $(SRC) src/state.h src/dedup.h
	$(CC) $(CFLAGS) -o $(BIN) $(SRC)

clean:
	rm -f $(BIN)

.PHONY: clean
