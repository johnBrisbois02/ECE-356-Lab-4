#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>


typedef struct{
    sem_t *runway;
    sem_t *control;
    int id;
    int takeoff_count;
    int queue_length;
    int* resource_queue;

} ThreadArgs;


int takeoff = 0;


void* proper(void* args_v){

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED,NULL);

    ThreadArgs *args = (ThreadArgs*) args_v;

    
    
    while(1){
        // control is used as queue semaphore instead here
        sem_wait(args->control);
        if(args->resource_queue[0] == args->id){
            
            printf("Thread[%d] Aquired Control tower permission\n",args->id);

            
            printf("Thread[%d] Aquired a runway\n",args->id);

            
            printf("Thread[%d] Taking off\n",args->id);
            takeoff++;
            args->takeoff_count++;

            int temp = args->resource_queue[0];
            for(int i = 0; i < args->queue_length-1; i++){

                args->resource_queue[i] = args->resource_queue[i+1];
                
                
            }
            args->resource_queue[args->queue_length-1] = temp;
            
            usleep(500000);
            
            sem_post(args->control);
        }else{
            sem_post(args->control);
            usleep(100000);
            
        }
    }

    return NULL;
}




void* starvation(void* args_v){

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED,NULL);

    ThreadArgs *args = (ThreadArgs*) args_v;

    
     
    while(1){

        sem_wait(args->control);
        printf("Thread[%d] Aquired Control tower permission\n",args->id);

        sem_wait(args->runway);
        printf("Thread[%d] Aquired a runway\n",args->id);
        
        printf("Thread[%d] Taking off\n",args->id);
        takeoff++;
        args->takeoff_count++;

        
        usleep(500000);
        
        sem_post(args->runway);
        sem_post(args->control);
        
    }

    return NULL;
}

void* deadlock(void* args_v){

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED,NULL);

    ThreadArgs *args = (ThreadArgs*) args_v;

    
     
    while(1){

        if(args->id%4 != 0){
            sem_wait(args->runway);
            printf("Thread[%d] Aquired a runway\n",args->id);
            usleep(10000);
            sem_wait(args->control);
            printf("Thread[%d] Aquired Control tower permission\n",args->id);

        }else{

            sem_wait(args->control);
            printf("Thread[%d] Aquired Control tower permission\n",args->id);
            usleep(10000);
            sem_wait(args->runway);
            printf("Thread[%d] Aquired a runway\n",args->id);

        }
        
        printf("Thread[%d] Taking off\n",args->id);
        takeoff++;
        args->takeoff_count++;
        usleep(500000);
        sem_post(args->runway);
        sem_post(args->control);
        
    }

    return NULL;
}

int main(){


    printf("Enter Scenario\n");
    printf("1 for deadlock, 2 for starvation, 3 for Proper resource managment\n");

    int resource_type = 5;
    if(scanf("%d", &resource_type) != 1){
        while(getchar() != '\n'){
        continue;
        }
    }
    while( resource_type > 3 || resource_type < 1){
        printf("Incorrect Input try again\n");
        if(scanf("%d", &resource_type) != 1){
            while(getchar() != '\n'){
                continue;
            }
        
        }
    }

    int thread_count = 15;
    printf("Enter number of Threads (Max:10 Min:1)\n");

    if(scanf("%d", &thread_count) != 1){
        while(getchar() != '\n'){
        continue;
        }
    }

    while( thread_count > 10 || thread_count < 1){
        printf("Incorrect Input try again\n");
        if(scanf("%d", &thread_count) != 1){
            while(getchar() != '\n'){
                continue;
            }
        
        }
    }

    sem_t runway; 
    sem_t control;

    sem_init(&runway, 0, 3);
    sem_init(&control, 0, 1);

    int queue_len = thread_count;

    int *queue = malloc(queue_len*sizeof(int));
    
    for(int i = 0; i < thread_count; i++){
        queue[i] = i+1;
        //printf("queue[%d] = %d\n",i,queue[i]);
    }
    

    pthread_t thread_arr[thread_count];

    ThreadArgs args[thread_count];


    for(int i = 0; i < thread_count; i++){
        args[i].id = i+1;
        args[i].runway = &runway;
        args[i].control = &control;
        args[i].takeoff_count = 0;
        args[i].queue_length = queue_len;
        args[i].resource_queue = queue;
        switch (resource_type) {

            case 1:
                pthread_create(&thread_arr[i],NULL,deadlock,(void*)&args[i]);
                break;
            case 2:
                pthread_create(&thread_arr[i],NULL,starvation,(void*)&args[i]);
                break;
            case 3:
                pthread_create(&thread_arr[i],NULL,proper,(void*)&args[i]);
                break;
            
        }
        

        
        
    }

    sleep(10);
    
    
    for(int i = 0; i < thread_count; i++){
        pthread_cancel(thread_arr[i]);
        pthread_join(thread_arr[i],NULL);
    }

    for(int i = 0; i < thread_count; i++){
       
        printf("Thread[%d] Total takeoff count:%d\n",args[i].id,args[i].takeoff_count);
    }
    free(queue);
    sem_destroy(&runway);
    sem_destroy(&control);
    printf("Takeoff count = %d\n",takeoff);
    return 0;
}