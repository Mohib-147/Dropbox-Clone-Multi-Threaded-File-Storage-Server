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


        char command_copy[1024];
        strcpy(command_copy, message);
        char *saveptr;
        char *cmd = strtok_r(command_copy, " \n", &saveptr);

        if (cmd != NULL && strcasecmp(cmd, "UPLOAD") == 0) {

            char *path  = strtok_r(NULL, " \n", &saveptr);
            char *extra = strtok_r(NULL, " \n", &saveptr);

            if (path == NULL || extra != NULL) {
                printf("Usage: UPLOAD <filename>\n");
                continue;
            }

            FILE *file = fopen(path, "rb");
            if (file == NULL) {
                perror("Could not open file");
                continue;
            }

            if (fseek(file, 0, SEEK_END) != 0) {
                perror("fseek");
                fclose(file);
                continue;
            }
            
            long file_size = ftell(file);
            rewind(file);

            if (file_size <= 0 || file_size > MAX_UPLOAD) {
                printf("File must be between 1 byte and %d bytes\n", MAX_UPLOAD);
                fclose(file);
                continue;
            }

            char *data = malloc((size_t)file_size);
            if (data == NULL) {
                printf("Out of memory\n");
                fclose(file);
                continue;
            }

            size_t bytes_read = fread(data, 1, (size_t)file_size, file);
            fclose(file);

            if (bytes_read != (size_t)file_size) {
                printf("Failed to read file\n");
                free(data);
                continue;
            }

            const char *name = strrchr(path, '/');
            name = (name != NULL) ? name + 1 : path;

            char upload_command[1024];
            snprintf(upload_command, sizeof(upload_command), "UPLOAD %s %ld\n", name, file_size);

            if (send_all(ClientSocket, upload_command, strlen(upload_command)) == -1) {
                printf("Failed to send command\n");
                free(data);
                break;
            }

            if (recv_all(ClientSocket, response, sizeof(response)) != 0) {
                printf("Server closed connection\n");
                free(data);
                break;
            }

            if (strcmp(response, "OK READY") != 0) {       
                printf("[Server] %s\n", response);
                free(data);
                continue;
            }

            if (send_all(ClientSocket, data, (size_t)file_size) == -1) {
                printf("Failed to send file data\n");
                free(data);
                break;
            }
            free(data);

            if (recv_all(ClientSocket, response, sizeof(response)) != 0) {
                printf("Server closed connection\n");
                break;
            }
            printf("[Server] %s\n", response);
            continue;
        }
        else if (cmd != NULL && strcasecmp(cmd, "LIST") == 0) {

            if (send_all(ClientSocket, message, strlen(message)) == -1) {
                printf("Failed to send message to server\n");
                break;
            }

            if (recv_all(ClientSocket, response, sizeof(response)) != 0) {
                printf("Server closed connection\n");
                break;
            }

            printf("[Server] %s\n", response);

            size_t n = 0;
            bool disconnected = false;

            if (sscanf(response, "OK %zu", &n) == 1) {
                for (size_t i = 0; i < n; i++) {
                    if (recv_all(ClientSocket, response, sizeof(response)) != 0) {
                        disconnected = true;
                        break;
                    }
                    printf("  %s\n", response);
                }
            }

            if (disconnected) {
                printf("Server closed connection\n");
                break;
            }
            continue;
        }
        else{

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



        
    }



    

    close(ClientSocket);

    return 0;

}