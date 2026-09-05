CC = cc
CFLAGS = -Wall -Wextra -g -O0
SRC = src/main.c src/net.c src/peer.c src/protocol.c src/state.c src/dedup.c src/log.c
BIN = auction

$(BIN): $(SRC) src/net.h src/peer.h src/protocol.h src/state.h src/dedup.h src/log.h
	$(CC) $(CFLAGS) -o $(BIN) $(SRC)

clean:
	rm -f $(BIN)

.PHONY: clean
