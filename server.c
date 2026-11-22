#include<stdio.h>	               
#include<stdlib.h>	
#include<unistd.h>
#include<sys/socket.h>	
#include<netinet/in.h>	
#include<arpa/inet.h>	
#include<pthread.h>	
#include<errno.h>	
#include<string.h>	
#include<stdbool.h>
#include "include/utils.h"
#include<sys/stat.h>


typedef struct {
    void **items; 
    size_t head, tail, count, cap;
    bool closed; 
    pthread_mutex_t lock;
    pthread_cond_t not_empty; 
    pthread_cond_t not_full;
} Queue;


int queue_push(Queue *q, void *item){
    
    pthread_mutex_lock(&q->lock);

    while (q->count == q->cap && !q->closed){
        pthread_cond_wait(&q->not_full, &q->lock);
    }
    
    if (q->closed) { 
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    
    q->items[q->tail] = item;
    q->tail = (q->tail+1)% q->cap;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    
    return 0;

}

void *queue_pop(Queue *q) {
    pthread_mutex_lock(&q->lock);

    while (q->count == 0 && !q->closed){
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    
    if (q->count == 0) { 
        pthread_mutex_unlock(&q->lock);
        return NULL;
    }
    void *item = q->items[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return item;
}

void queue_close(Queue *q){

    pthread_mutex_lock(&q->lock);

    q->closed=true;

    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);


    pthread_mutex_unlock(&q->lock);
    
} 



typedef struct {
    char username[32];
    char password[128];

    size_t quota_used;
    size_t quota_limit;

} User;

User *users = NULL;
size_t user_count = 0;
pthread_mutex_t user_lock;

int add_user(char *username, char *password)
{
    pthread_mutex_lock(&user_lock);

    for (size_t i = 0; i < user_count; i++) {
        if (strcmp(users[i].username, username) == 0) {
            pthread_mutex_unlock(&user_lock);
            return 1;  
        }
    }

    User *temp = realloc(users, (user_count + 1) * sizeof(User));

    if (temp == NULL) {
        pthread_mutex_unlock(&user_lock);
        return -1;  
    }

    users = temp;

    strcpy(users[user_count].username, username);
    strcpy(users[user_count].password, password);

    users[user_count].quota_used = 0;
    users[user_count].quota_limit = 1024 * 1024;

    user_count++;

    pthread_mutex_unlock(&user_lock);

    return 0;
}



void handleclient(int client_fd)
{
    printf("Connected to client with fd: %d\n\n", client_fd);

    bool authenticated = false;
    char logged_in_user[32] = {0};

    while (true)
    {
        char buffer[1024] = {0};
        int rc = recv_all(client_fd, buffer, sizeof(buffer));
        if (rc == -1) {
            perror("recv failed in handleclient");
            return;
        }
        if (rc == 1) {
            printf("Client %d disconnected\n", client_fd);
            return;
        }

        char *saveptr;
        char *token = strtok_r(buffer, " ", &saveptr);

        if (token == NULL)
            continue;

        // QUIT =================================================
        if (strcasecmp(token, "QUIT") == 0) {

            char *extra = strtok_r(NULL, " ", &saveptr);

            if (extra != NULL) {
                send_response(client_fd, "ERR BADCMD\n");
                continue;
            }

            send_response(client_fd, "OK bye\n");
            break;
        }

        // SIGNUP ===============================================
        else if (strcasecmp(token, "SIGNUP") == 0) {

            char *username = strtok_r(NULL, " ", &saveptr);
            char *password = strtok_r(NULL, " ", &saveptr);
            char *extra = strtok_r(NULL, " ", &saveptr);

            if (username == NULL || password == NULL || extra != NULL) {
                send_response(client_fd, "ERR BADCMD\n");
                continue;
            }

            if (!validate_username(username)) {
                send_response(client_fd, "Wrong Username Format\n");
                continue;
            }

            if (!validate_password(password)) {
                send_response(client_fd, "Wrong Password Format\n");
                continue;
            }

            char user_path[64];
            char identity_path[96];

            snprintf(user_path, sizeof(user_path), "storage/%s", username);
            snprintf(identity_path, sizeof(identity_path),"%s/identity.txt", user_path);

            if (mkdir(user_path, 0755) == -1) {
                send_response(client_fd, "ERR EXISTS\n");
                continue;
            }

            FILE *file = fopen(identity_path, "w");

            if (file == NULL) {
                perror("fopen");

                rmdir(user_path);

                send_response(client_fd, "ERR INTERNAL\n");
                continue;
            }

            size_t used_q = 0;
            size_t q_limit = 1024 * 1024;

            bool write_failed = false;

            if (fprintf(file, "%s\n%s\n%zu\n%zu",
                        username, password, used_q, q_limit) < 0) {
                write_failed = true;
            }

            if (fclose(file) != 0) {
                write_failed = true;
            }

            if (write_failed) {
                unlink(identity_path);
                rmdir(user_path);

                send_response(client_fd, "ERR INTERNAL\n");
                continue;
            }

            int add_status = add_user(username, password);

            if (add_status != 0) {

                unlink(identity_path);
                rmdir(user_path);

                if (add_status == 1) {
                    send_response(client_fd, "ERR EXISTS\n");
                }
                else {
                    send_response(client_fd, "ERR INTERNAL\n");
                }

                continue;
            }

            send_response(client_fd, "OK signed up\n");
            continue;
        }

        // LOGIN =================================================
        else if (strcasecmp(token, "LOGIN") == 0) {

            char *username = strtok_r(NULL, " ", &saveptr);
            char *password = strtok_r(NULL, " ", &saveptr);
            char *extra = strtok_r(NULL, " ", &saveptr);

            if (username == NULL || password == NULL || extra != NULL) {
                send_response(client_fd, "ERR BADCMD\n");
                continue;
            }

            if (authenticated) {
                send_response(client_fd, "ERR AUTH\n");
                continue;
            }

            char stored_hash[128] = {0};
            bool user_found = false;

            pthread_mutex_lock(&user_lock);

            for (size_t i = 0; i < user_count; i++) {

                if (strcmp(users[i].username, username) == 0) {
                    strcpy(stored_hash, users[i].password);
                    user_found = true;
                    break;
                }
            }

            pthread_mutex_unlock(&user_lock);

            if (user_found && verify_password(password)) {

                authenticated = true;
                strcpy(logged_in_user, username);

                send_response(client_fd, "OK logged in\n");
            }
            else {
                send_response(client_fd, "ERR AUTH\n");
            }
        }

        else {

            if (!authenticated) {
                send_response(client_fd, "ERR NOAUTH\n");
                continue;
            }

            send_response(client_fd, "ERR BADCMD\n");
        }
    }

    return;
}











void* clientThread(void* arg){

    Queue* q = arg;

    while(true){
        
        void* client_ptr = queue_pop(q);
        if(client_ptr == NULL){
            return NULL;
        }
    
        int client_fd = *(int *)client_ptr;

        handleclient(client_fd);

        close(client_fd);
        free(client_ptr);
    }

    return NULL;


}



int main(int argc, char* argv[]){

    //=========================================================================Socket Part========================================================

    if(argc!=2){
        perror("More than 1 argument provided");
        return 1;
    }

    int port = atoi(argv[1]);

    int ServerSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(ServerSocket<0){
        perror("Server Socket Creation Failed");
        return 1;
    }

    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if((bind(ServerSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)))!=0){
        perror("Server Failed to Bind with Socket");
        close(ServerSocket);
        return 1;
    }

    printf("Server Active on IP 127.0.0.1 Port 8080\n\n");

    if(listen(ServerSocket, 100)==-1){
        perror("Server Failed to Listen");
        return 1;
    }  

    printf("Listening for Clients.............\n\n");


    //==================================================Socket Part==========================================================


    

    Queue Q;
    Q.cap=10; Q.closed=false; Q.count=0; Q.head=0; Q.tail=0;
    pthread_mutex_init(&Q.lock, NULL);
    pthread_cond_init(&Q.not_empty, NULL);
    pthread_cond_init(&Q.not_full, NULL);

    Q.items = malloc(Q.cap * sizeof(void*));

    if(Q.items==NULL){
        perror("Failed to Assign memory to Q.items");
        return 1;
    }


    pthread_t  clientthreads[4];

    for(int i=0; i<4; i++){
        if(pthread_create(&clientthreads[i], NULL, clientThread, &Q)==0){
            printf("Successfully Initialized Worker %d\n", i);
        }
        else{
            printf("Failed to Initialized Worker %d\n", i);
        }
    
    }


    struct sockaddr_in client_addr;
    
    while(true){



        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(ServerSocket,(struct sockaddr *)&client_addr,&client_len);
    
        if(client_fd == -1){
            perror("Failed to Accept client in Server, Trying Again");
            continue;
        }

        int *client_ptr = malloc(sizeof(int));
        *client_ptr = client_fd;

        if(queue_push(&Q, client_ptr)==-1){
            fprintf(stderr,"failed to push Client into Queue with fd: %d", client_fd);
                close(client_fd);
                free(client_ptr);
            continue;
        };

    }


    for(int i=0; i<4; i++){
        pthread_join(clientthreads[i], NULL);
    }
    
    

}