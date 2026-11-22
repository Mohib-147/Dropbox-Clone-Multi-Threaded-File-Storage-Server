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



int main(int argc, char* argv[]){
    if(argc!=3){
        perror("Wrong input format");
        return 1;
    }

    int ServerPort = atoi(argv[2]);
    

    int ClientSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (ClientSocket < 0) {
        perror("Client Socket Creation Failed");
        return 1;
    }

    struct sockaddr_in client_addr;

    memset(&client_addr, 0, sizeof(client_addr));

    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(ServerPort);
    client_addr.sin_addr.s_addr = INADDR_ANY;

    if (inet_pton(AF_INET, argv[1], &client_addr.sin_addr) <= 0) {
        perror("Invalid IP address");
        close(ClientSocket);
        return 1;
    }

    if (connect(ClientSocket, (struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) {
        perror("Connection failed");
        close(ClientSocket);
        return 1;
    }

    printf("Connected to Server with Port %d\n\n", ServerPort);


    char message[1024];

    while (true) {

        printf(":> ");

        if (fgets(message, sizeof(message), stdin) == NULL)
            break;

        if (send_all(ClientSocket, message, strlen(message)) == -1) {
            printf("Failed to send message to server\n");
            break;
        }

        if (strcmp(message, "QUIT\n") == 0 || strcmp(message, "quit\n") == 0) break;
        
    }



    

    close(ClientSocket);

    return 0;

}