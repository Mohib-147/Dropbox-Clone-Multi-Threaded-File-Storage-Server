CC = gcc

CFLAGS = -std=c11 -D_DEFAULT_SOURCE -Wall -Wextra -Werror -g -O0
THREAD_FLAGS = -pthread

SRC_DIR = src
INC_DIR = include

SERVER = server
CLIENT = client

.PHONY: all server client clean

all: server client

server: server.c include/utils.c include/utils.h
	$(CC) $(CFLAGS) $(THREAD_FLAGS) -I$(INC_DIR) server.c include/utils.c -o $(SERVER)

client: client.c include/utils.c include/utils.h
	$(CC) $(CFLAGS) $(THREAD_FLAGS) -I$(INC_DIR) client.c include/utils.c -o $(CLIENT)

clean:
	rm -f $(SERVER) $(CLIENT)