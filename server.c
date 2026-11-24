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
    char *filename;
    size_t size;
} User_File;


typedef struct {
    char username[32];
    char password[128];

    size_t quota_used;
    size_t quota_limit;

    User_File *files;
    size_t file_count;

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

    users[user_count].files = NULL;
    users[user_count].file_count = 0;

    user_count++;

    pthread_mutex_unlock(&user_lock);

    return 0;
}

User *find_user(const char *username)
{
    for (size_t i = 0; i < user_count; i++) {
        if (strcmp(users[i].username, username) == 0) {
            return &users[i];
        }
    }
    return NULL;
}


int find_file(User *u, const char *filename){
    for (size_t i = 0; i < u->file_count; i++) {
        if (strcmp(u->files[i].filename, filename) == 0) {
            return (int)i;
        }
    }
    return -1;
}


int add_file(User *u, const char *filename, size_t size){
    User_File *temp = realloc(u->files, (u->file_count + 1) * sizeof(User_File));

    if (temp == NULL) {
        return -1;
    }

    u->files = temp;

    u->files[u->file_count].filename = strdup(filename);

    if (u->files[u->file_count].filename == NULL) {
        return -1;
    }

    u->files[u->file_count].size = size;
    u->file_count++;

    return 0;
}


void remove_file(User *u, int index){
    free(u->files[index].filename);

    u->files[index] = u->files[u->file_count - 1];
    u->file_count--;
}


int save_identity(User *u){
    char identity_path[96];

    snprintf(identity_path, sizeof(identity_path), "storage/%s/identity.txt", u->username);

    FILE *file = fopen(identity_path, "w");

    if (file == NULL) {
        perror("save_identity fopen");
        return -1;
    }

    fprintf(file, "%s\n%s\n%zu\n%zu\n", u->username, u->password, u->quota_used, u->quota_limit);

    for (size_t i = 0; i < u->file_count; i++) {
        fprintf(file, "%s %zu\n", u->files[i].filename, u->files[i].size);
    }

    if (fclose(file) != 0) {
        return -1;
    }

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
                    send_response(client_fd, "OK logged in\n");
                    break;
                }
            }
            pthread_mutex_unlock(&user_lock);

            if(!authenticated){
                send_response(client_fd, "ERR AUTH\n");
            }

        }

        // UPLOAD =================================================
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

            bool exists = false;
            bool over_quota = false;

            pthread_mutex_lock(&user_lock);
            User *u = find_user(logged_in_user);
            if (u != NULL) {
                exists = (find_file(u, filename) != -1);
                over_quota = (u->quota_used + (size_t)size > u->quota_limit);
            }
            pthread_mutex_unlock(&user_lock);

            if (exists) {
                send_response(client_fd, "ERR EXISTS\n");
                continue;
            }

            if (over_quota) {
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
            T->payload = payload;
            T->payload_len = (size_t)size;
            T->status = -1;
            T->result = NULL;
            T->result_len = 0;
            T->done = 0;
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
                int add_status = -1;

                pthread_mutex_lock(&user_lock);
                u = find_user(logged_in_user);
                if (u != NULL && add_file(u, T->filename, T->payload_len) == 0) {
                    u->quota_used += T->payload_len;
                    save_identity(u);
                    add_status = 0;
                }
                pthread_mutex_unlock(&user_lock);

                if (add_status == 0) {
                    send_response(client_fd, "OK uploaded\n");
                }
                else {
                    send_response(client_fd, "ERR INTERNAL\n");
                }
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

        // DELETE =================================================
        else if(strcasecmp(token, "DELETE")==0){

            if (!authenticated) {
                send_response(client_fd, "ERR NOAUTH\n");
                continue;
            }

            char *filename = strtok_r(NULL, " ", &saveptr);
            char *extra    = strtok_r(NULL, " ", &saveptr);

            if (filename == NULL || extra != NULL) {
                send_response(client_fd, "ERR BADCMD\n");
                continue;
            }

            if (!validate_filename(filename)) {
                send_response(client_fd, "ERR NOTFOUND\n");
                continue;
            }

            bool found = false;

            pthread_mutex_lock(&user_lock);
            User *u = find_user(logged_in_user);
            if (u != NULL) {
                found = (find_file(u, filename) != -1);
            }
            pthread_mutex_unlock(&user_lock);

            if (!found) {
                send_response(client_fd, "ERR NOTFOUND\n");
                continue;
            }

            Task *T  = calloc(1, sizeof(Task));
            if (T == NULL) {
                send_response(client_fd, "ERR INTERNAL\n");
                continue;
            }

            T->cmd = CMD_DELETE;
            snprintf(T->user, sizeof(T->user), "%s", logged_in_user);
            snprintf(T->filename, sizeof(T->filename), "%s", filename);
            T->payload  = NULL;
            T->payload_len = 0;
            T->status = -1;
            T->result = NULL;
            T->result_len = 0;
            T->done = 0;
            pthread_mutex_init(&T->lock, NULL);
            pthread_cond_init(&T->done_cv, NULL);

            if (queue_push(&task_queue, T) != 0) {
                pthread_mutex_destroy(&T->lock);
                pthread_cond_destroy(&T->done_cv);
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

                pthread_mutex_lock(&user_lock);
                u = find_user(logged_in_user);
                if (u != NULL) {
                    int index = find_file(u, T->filename);
                    if (index != -1) {
                        size_t fsize = u->files[index].size;
                        u->quota_used = (u->quota_used >= fsize) ? u->quota_used - fsize : 0;
                        remove_file(u, index);
                        save_identity(u);
                    }
                }
                pthread_mutex_unlock(&user_lock);

                send_response(client_fd, "OK deleted\n");
            }
            else {
                send_response(client_fd, "ERR INTERNAL\n");
            }

            pthread_mutex_destroy(&T->lock);
            pthread_cond_destroy(&T->done_cv);
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

        // File stays open: the file list comes after the header
        if (!valid) {
            fprintf(stderr, "Invalid identity file for %s\n", entry->d_name);
            fclose(file);
            continue;
        }

        username[strcspn(username, "\n")] = '\0';
        password[strcspn(password, "\n")] = '\0';

        if (!validate_username(username) || !validate_password(password)) {
            fprintf(stderr, "Invalid user information for %s\n", entry->d_name);
            fclose(file);
            continue;
        }

        int status = add_user(username, password);

        if (status == -1) {
            fclose(file);
            closedir(dir);
            return -1;
        }

        if (status == 1) {
            fclose(file);
            continue;
        }

        pthread_mutex_lock(&user_lock);

        User *u = find_user(username);

        if (u != NULL) {
            u->quota_used = used_q;
            u->quota_limit = q_limit;

            // Remaining lines: "<filename> <size>"
            char fname[64];
            size_t fsize;

            while (fscanf(file, "%63s %zu", fname, &fsize) == 2) {

                if (!validate_filename(fname)) {
                    fprintf(stderr, "Skipping invalid file entry %s for %s\n", fname, username);
                    continue;
                }

                if (add_file(u, fname, fsize) != 0) {
                    fprintf(stderr, "Out of memory loading files for %s\n", username);
                    break;
                }
            }
        }

        pthread_mutex_unlock(&user_lock);

        fclose(file);

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

        FILE *f = fopen(path, "wbx");       // 'x': fail if the file already exists
        if (f != NULL) {
            if (fwrite(T->payload, 1, T->payload_len, f) == T->payload_len)
                status = 0;
            fclose(f);
        }
    }
    else if(T->cmd == CMD_DELETE){
        char path[160];
        snprintf(path, sizeof(path), "storage/%s/%s", T->user, T->filename);

        // Already missing on disk counts as deleted, so memory can be cleaned up
        if (unlink(path) == 0 || errno == ENOENT)
            status = 0;
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

    printf("Server Active on IP 127.0.0.1 Port %d\n\n", port);

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