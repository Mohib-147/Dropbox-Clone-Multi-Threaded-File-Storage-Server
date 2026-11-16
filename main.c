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



int main(int argc, char* argv[]){

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

    if(listen(ServerSocket, 100)==-1){
        perror("Server Failed to Listen");
        return 1;
    }  //backlog, tells how many connections can wait.


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



    struct sockaddr_in client_addr;
    
    while(true){

        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(ServerSocket,(struct sockaddr *)&client_addr,&client_len);
    
        if(client_fd == -1){
            perror("Failed to Accept client in Server, Trying Again");
            continue;
        }

    }
    











}