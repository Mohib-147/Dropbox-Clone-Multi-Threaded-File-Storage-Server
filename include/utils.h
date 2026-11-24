#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>
#include<string.h>
#include<stdbool.h>
#include<ctype.h>

#define MAX_UPLOAD (256 * 1024)


int send_all(int socket, char* message, size_t len);
int recv_all(int client_fd, char *buffer, size_t capacity);
int recv_exact(int fd, char *buf, size_t len);
void send_response(int client_fd, char *message);

bool validate_username(char *username);
bool validate_password(char *password);
bool validate_filename(char *filename);

bool verify_password(const char *password);

#endif