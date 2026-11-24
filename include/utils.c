#include "../include/utils.h"




int send_all(int socket, char* message, size_t len){
    size_t bytes_sent = 0;

    while(bytes_sent<len){
        ssize_t sent= send(socket, message + bytes_sent, len - bytes_sent, 0);

        if (sent <= 0) {
            if (sent < 0 && errno == EINTR) {
                continue;
            }
            return -1;
        }
            bytes_sent += sent;
    }

    return 0;
}

int recv_all(int client_fd, char *buffer, size_t capacity)
{
    size_t received = 0;
    char ch;

    while (received < capacity - 1)
    {
        ssize_t n = recv(client_fd, &ch, 1, 0);

        if (n == 0)
            return 1;  

        if (n < 0){
            if (errno == EINTR)
                continue;

            return -1;
        }

        if (ch == '\n')
            break;

        buffer[received++] = ch;
    }

    buffer[received] = '\0';

    return 0;
}


int recv_exact(int fd, char *buf, size_t len){
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, buf + got, len - got, 0);
        if (n == 0) return 1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)n;
    }
    return 0;
}

void send_response(int client_fd, char *message){
    send_all(client_fd, message, strlen(message));
}



bool validate_username(char *username) {
    if (username == NULL) {
        return false;
    }
    
    size_t len = 0;
    
    for (int i = 0; username[i] != '\0'; i++) {
        if (len >= 31) {
            return false;
        }
        if (!isalnum((unsigned char)username[i])) {
            return false;
        }
        len++;
    }
    return len >= 3;
}


bool validate_password(char *password){
    if (password == NULL)
        return false;

    size_t len = strlen(password);

    if (len < 8 || len > 72)
        return false;

    for (size_t i = 0; i < len; i++) {
        if (password[i] == ' ' || password[i] == '\t' ||
            password[i] == '\n' || password[i] == '\r')
            return false;
    }

    return true;
}


bool verify_password(const char *password){
    (void)password;
    return true;
}


bool validate_filename(char *filename)
{
    size_t len = strlen(filename);

    if (len == 0 || len > 63)
        return false;

    if (strstr(filename, "..") != NULL)
        return false;

    for (size_t i = 0; i < len; i++) {
        char c = filename[i];

        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-')) {
            return false;
        }
    }

    return true;
}


