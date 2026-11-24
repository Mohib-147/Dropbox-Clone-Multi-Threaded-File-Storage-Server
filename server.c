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
#include <dirent.h>


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

Queue task_queue;


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


typedef enum { CMD_UPLOAD, CMD_DOWNLOAD, CMD_DELETE, CMD_LIST } Command;

typedef struct  {
    Command cmd;
    char user[32];
    char filename[64];
    char *payload; /* UPLOAD: bytes read from the socket */
    size_t payload_len;
    /* ---- filled in by the worker ---- */
    int status; /* 0 = ok, otherwise an error code */
    char *result; /* DOWNLOAD bytes, LIST text */
    size_t result_len;
    /* ---- completion signalling: one possible design (Problem 1, option A) ---- */
    pthread_mutex_t lock;
    pthread_cond_t done_cv;
    int done;
} Task;


void handleclient(int client_fd){
    printf("Connected to client with fd: %d\n\n", client_fd);

    bool authenticated = false;
    char logged_in_user[32] = {0};
    size_t user_quota_used = 0;
    size_t user_maximum_quota = 0;

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


            pthread_mutex_lock(&user_lock);

            for (size_t i = 0; i < user_count; i++) {

                if (strcmp(users[i].username, username) == 0 && strcmp(users[i].password, password)==0) {
                    authenticated = true;
                    strcpy(logged_in_user, users[i].username);
                    user_quota_used = users[i].quota_used;
                    user_maximum_quota = users[i].quota_limit;
                    send_response(client_fd, "OK logged in\n");
                    break;
                }
            }
            pthread_mutex_unlock(&user_lock);

            if(!authenticated){
                send_response(client_fd, "ERR AUTH\n");
            }

        }

        else if (strcasecmp(token, "UPLOAD") == 0) {

            if (!authenticated) {
                send_response(client_fd, "ERR NOAUTH\n");
                continue;
            }

            char *filename = strtok_r(NULL, " ", &saveptr);
            char *sz       = strtok_r(NULL, " ", &saveptr);
            char *extra    = strtok_r(NULL, " ", &saveptr);

            if (filename == NULL || sz == NULL || extra != NULL) {
                send_response(client_fd, "ERR BADCMD\n");
                continue;
            }

            if (!validate_filename(filename)) {
                send_response(client_fd, "ERR BADNAME\n");
                continue;
            }

            char *endptr;
            errno = 0;
            long size = strtol(sz, &endptr, 10);
            if (errno != 0 || *endptr != '\0' || size <= 0 || size > MAX_UPLOAD) {
                send_response(client_fd, "ERR SIZE\n");
                continue;
            }

            if (user_quota_used + (size_t)size > user_maximum_quota) {
                send_response(client_fd, "ERR QUOTA\n");
                continue;
            }

            Task *T  = calloc(1, sizeof(Task));
            char *payload = malloc((size_t)size);
            if (T == NULL || payload == NULL) {
                free(T);
                free(payload);
                send_response(client_fd, "ERR INTERNAL\n");
                continue;
            }

            send_response(client_fd, "OK READY\n");

            int rc = recv_exact(client_fd, payload, (size_t)size);
            if (rc != 0) {
                printf("Upload from %s aborted (client %d)\n", logged_in_user, client_fd);
                free(payload);
                free(T);
                return;
            }

            T->cmd = CMD_UPLOAD;
            snprintf(T->user, sizeof(T->user), "%s", logged_in_user);
            snprintf(T->filename, sizeof(T->filename), "%s", filename);
            T->payload     = payload;
            T->payload_len = (size_t)size;
            T->status      = -1;
            T->result      = NULL;
            T->result_len  = 0;
            T->done        = 0;
            pthread_mutex_init(&T->lock, NULL);
            pthread_cond_init(&T->done_cv, NULL);

            if (queue_push(&task_queue, T) != 0) {          
                pthread_mutex_destroy(&T->lock);
                pthread_cond_destroy(&T->done_cv);
                free(T->payload);
                free(T);
                send_response(client_fd, "ERR INTERNAL\n");
                continue;
            }

            pthread_mutex_lock(&T->lock);
            while (!T->done){
                pthread_cond_wait(&T->done_cv, &T->lock);
            }
            pthread_mutex_unlock(&T->lock);

            if (T->status == 0) {
                user_quota_used += (size_t)size;

                pthread_mutex_lock(&user_lock);
                for (size_t i = 0; i < user_count; i++) {
                    if (strcmp(users[i].username, logged_in_user) == 0) {
                        users[i].quota_used = user_quota_used;

                        char id_path[96];
                        snprintf(id_path, sizeof(id_path), "storage/%s/identity.txt", logged_in_user);
                        FILE *idf = fopen(id_path, "w");
                        if (idf != NULL) {
                            fprintf(idf, "%s\n%s\n%zu\n%zu", users[i].username, users[i].password,
                                    users[i].quota_used, users[i].quota_limit);
                            fclose(idf);
                        }
                        break;
                    }
                }
                pthread_mutex_unlock(&user_lock);

                send_response(client_fd, "OK uploaded\n");
            }
            else if (T->status == 2) {
                send_response(client_fd, "ERR QUOTA\n");
            }
            else {
                send_response(client_fd, "ERR INTERNAL\n");
            }

            pthread_mutex_destroy(&T->lock);
            pthread_cond_destroy(&T->done_cv);
            free(T->payload);
            free(T->result);
            free(T);
            continue;
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


int loadUsers()
{
    const char *base_path = "storage/";

    DIR *dir = opendir(base_path);

    if (dir == NULL) {
        if (errno == ENOENT) {
            return 0;  
        }

        perror("opendir");
        return -1;
    }

    struct dirent *entry;
    int loaded = 0;

    while ((entry = readdir(dir)) != NULL) {

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char user_path[512];

        snprintf(user_path, sizeof(user_path),"%s%s", base_path, entry->d_name);

        struct stat info;

        if (stat(user_path, &info) == -1 || !S_ISDIR(info.st_mode)) {
            continue;
        }

        char identity_path[1024];

        snprintf(identity_path, sizeof(identity_path),"%s/identity.txt", user_path);

        FILE *file = fopen(identity_path, "r");

        if (file == NULL) {
            fprintf(stderr, "Could not open identity file for %s\n",entry->d_name);
            continue;
        }

        char username[32];
        char password[128];

        size_t used_q;
        size_t q_limit;

        bool valid = fgets(username, sizeof(username), file) != NULL && fgets(password, sizeof(password), file) != NULL && fscanf(file, "%zu\n%zu", &used_q, &q_limit) == 2;

        fclose(file);

        if (!valid) {
            fprintf(stderr, "Invalid identity file for %s\n", entry->d_name);
            continue;
        }

        username[strcspn(username, "\n")] = '\0';
        password[strcspn(password, "\n")] = '\0';

        if (!validate_username(username) || !validate_password(password)) { 
            fprintf(stderr, "Invalid user information for %s\n", entry->d_name);
            continue;
        }

        int status = add_user(username, password);

        if (status == -1) {
            closedir(dir);
            return -1;
        }

        if (status == 1) {
            continue;  
        }

        pthread_mutex_lock(&user_lock);

        for (size_t i = 0; i < user_count; i++) {
            if (strcmp(users[i].username, username) == 0) {
                users[i].quota_used = used_q;
                users[i].quota_limit = q_limit;
                break;
            }
        }

        pthread_mutex_unlock(&user_lock);

        loaded++;
    }

    closedir(dir);

    return loaded;
}








void handletask(Task *T)
{
    int status = -1;

    if (T->cmd == CMD_UPLOAD) {
        char path[160];
        snprintf(path, sizeof(path), "storage/%s/%s", T->user, T->filename);

        FILE *f = fopen(path, "wb");
        if (f != NULL) {
            if (fwrite(T->payload, 1, T->payload_len, f) == T->payload_len)
                status = 0;
            fclose(f);
        }
    }

    pthread_mutex_lock(&T->lock);
    T->status = status;
    T->done = 1;
    pthread_cond_signal(&T->done_cv);
    pthread_mutex_unlock(&T->lock);
}




void* taskThread(void* arg) {

    Queue* q = arg;

    while (true) {

        Task *T = queue_pop(q);

        if (T == NULL) {
            return NULL;
        }

        handletask(T);
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










    //=========================================================USER + Client handeling===================================================
    printf("Loading Users................\n\n");

    pthread_mutex_init(&user_lock, NULL);
    
    if (loadUsers() == -1) {
        fprintf(stderr, "Failed to load users\n");
        return 1;
    }

    printf("Loaded %zu users\n", user_count);

    Queue client_queue;
    client_queue.cap=10; client_queue.closed=false; client_queue.count=0; client_queue.head=0; client_queue.tail=0;
    pthread_cond_init(&client_queue.not_empty, NULL);
    pthread_mutex_init(&client_queue.lock, NULL);
    pthread_cond_init(&client_queue.not_full, NULL);

    client_queue.items = malloc(client_queue.cap * sizeof(void*));

    if(client_queue.items==NULL){
        perror("Failed to Assign memory to client_queue.items");
        return 1;
    }


    pthread_t  clientthreads[4];

    for(int i=0; i<4; i++){
        if(pthread_create(&clientthreads[i], NULL, clientThread, &client_queue)!=0){
            printf("Failed to Client Threads %d\n", i);
        }
    }
    
    //=========================================================USER + Client handeling===========================================================
    




    //=========================================================Task handeling===========================================================
    task_queue.cap=20; task_queue.closed=false; task_queue.count=0; task_queue.head=0; task_queue.tail=0;
    pthread_cond_init(&task_queue.not_empty, NULL);
    pthread_mutex_init(&task_queue.lock, NULL);
    pthread_cond_init(&task_queue.not_full, NULL);
    
    task_queue.items = malloc(task_queue.cap * sizeof(void*));

    if(task_queue.items==NULL){
        perror("Failed to Assign memory to task_queue.items");
        return 1;
    }

    pthread_t  taskthreads[4];
    for(int i=0; i<4; i++){
        if(pthread_create(&taskthreads[i], NULL, taskThread, &task_queue)!=0){
            printf("Failed to Client Threads %d\n", i);
        }
    }   
    //=========================================================Task handeling===========================================================

    
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

        if(queue_push(&client_queue, client_ptr)==-1){
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