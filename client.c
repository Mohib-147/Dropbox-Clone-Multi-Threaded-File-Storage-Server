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
#include "include/utils.h"



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
    char response[1024];

    while (true) {

        printf(":> ");

        if (fgets(message, sizeof(message), stdin) == NULL)
            break;

        if (send_all(ClientSocket, message, strlen(message)) == -1) {
            printf("Failed to send message to server\n");
            break;
        }

        if(recv_all(ClientSocket, response, sizeof(response))==-1){
            printf("Failed to recieve message from server\n");
        }
        printf("[Server] %s\n", response);
        
        if (strcmp(message, "QUIT\n") == 0 || strcmp(message, "quit\n") == 0) break;
        
    }



    

    close(ClientSocket);

    return 0;

}