#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>


typedef struct {
char id;
int balance;
pthread_mutex_t lock;
} Account;

typedef struct{

Account* account_ary;
int T_id;
int tot_act;
int*req_act;


} ThreadArg;

typedef struct{

pthread_t* thread_ar;
int** cycle_ar;
int tot_thread;

} CycleArg;

typedef struct{

    int act_tot;
    
    int holder[64];//high number to accomodate all accounts

    int need[64][2];//2 for number of locks per thread

    pthread_mutex_t bank_lock;

}Banker;

static Banker banker;


void BankerInit(int accounts_tot){

    banker.act_tot = accounts_tot;
    for(int i = 0; i<accounts_tot; i++){
        banker.holder[i] = 0;
    }
    pthread_mutex_init(&banker.bank_lock,NULL);
}


void BankerNeeds(int thread_id, int need_1, int need_2){
    banker.need[thread_id][0] = need_1;
    banker.need[thread_id][1] = need_2;
}

void BankerPermission(int thread_id){
    
    
    int waiting = 1;
    int need_1 = banker.need[thread_id][0];
    int need_2 = banker.need[thread_id][1];
    while(waiting){
        if(banker.holder[need_1] == 0 && banker.holder[need_2] == 0){
            pthread_mutex_lock(&banker.bank_lock);
            printf("Thread[%d] got Banker's permission\n",thread_id+1);
            waiting = 0;
            banker.holder[need_1] = 1;
            banker.holder[need_2] = 1;
        }
        else{
            usleep(500000);
        }

    }
    pthread_mutex_unlock(&banker.bank_lock);
}

void BankerReturn(int thread_id){
    printf("Thread[%d] returning locks to Banker\n",thread_id+1);
    int need_1 = banker.need[thread_id][0];
    int need_2 = banker.need[thread_id][1];
    pthread_mutex_lock(&banker.bank_lock);
    banker.holder[need_1] = 0;
    banker.holder[need_2] = 0;
    pthread_mutex_unlock(&banker.bank_lock);
}

void* CycleDetect(void* cyc_ar){
    //sleep(1);
    
    int cyc = 1;
    CycleArg* cyc_arg = (CycleArg*) cyc_ar;
    int in_cyc[cyc_arg->tot_thread];
    for(int i = 0; i<cyc_arg->tot_thread;i++){
        in_cyc[i] = 0;
    }
    int cur_start = 0;
    for(int i = 0; i<cyc_arg->tot_thread;i++){
        cyc = 1;
        cur_start = i;
        int counter = 0;
        while(cyc){
            //printf("Cur_start:%d\n",cur_start);
            int *cur_thr_ids = (cyc_arg->cycle_ar[cur_start]);
            //printf("cur_thr_ids 0:%d, cur_thr_ids 1:%d\n",cur_thr_ids[0],cur_thr_ids[1]);
            //printf("in_cyc 0:%d, in_cyc 1:%d,in_cyc 2:%d,in_cyc 3:%d\n",in_cyc[0],in_cyc[1],in_cyc[2],in_cyc[3]);
            if (in_cyc[cur_thr_ids[0]] == 1){
                printf("Cycle found terminating Thread[%d]\n",cur_start+1);
                pthread_cancel(cyc_arg->thread_ar[cur_start]);
                pthread_join(cyc_arg->thread_ar[cur_start],NULL);
                cyc = 0;
                in_cyc[cur_start] = 2;
            }
            else if(in_cyc[cur_thr_ids[0]] == 2){
                cyc = 0;
            }else{
                in_cyc[cur_thr_ids[0]] = 1;
                if(counter > cyc_arg->tot_thread){
                    cyc = 0;
                }else if (cur_thr_ids[0] == cur_thr_ids[1]){
                    cyc = 0;
                }
                counter++;
                cur_start = cur_thr_ids[1];
                
            }
        }
        for(int k = 0; k<cyc_arg->tot_thread;k++){
            if(in_cyc[k] == 1){
                in_cyc[k] = 0;
            }
        }
    }
    

    return NULL;
}







void* Phase2(void* args){
    ThreadArg *cur_arg = (ThreadArg*)args;
    int thread_num = cur_arg->T_id;

    int *act_list = cur_arg->req_act;
    //int thread_type = thread_num%4;

    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id);
    pthread_mutex_lock(&(cur_arg->account_ary[act_list[0]].lock));
    printf("Thread[%d] Succesfully Locked account %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id);
    sleep(1);

    switch(thread_num){

        case 0:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 200;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 200;
            break;
        case 1:
            //cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 200;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 100;
            break;
        case 2:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 150;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 150;
            break;
        case 3:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 50;
            //cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 200;
            break;
        default:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + thread_num;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - thread_num;
            break;
    }


    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[act_list[1]].id);
    pthread_mutex_lock(&(cur_arg->account_ary[act_list[1]].lock));
    printf("Thread[%d] locked accounts %c and %c Transfering now\n",thread_num+1,cur_arg->account_ary[act_list[0]].id,cur_arg->account_ary[act_list[1]].id );
    // perform transaction here
    printf("Thread[%d] Transfer Complete unlocking accounts %c and %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id,cur_arg->account_ary[act_list[1]].id );
    pthread_mutex_unlock(&(cur_arg->account_ary[act_list[1]].lock));
    pthread_mutex_unlock(&(cur_arg->account_ary[act_list[0]].lock));

    
    return 0;
}



void* Phase1(void* args){
    ThreadArg *cur_arg = (ThreadArg*)args;
    int thread_num = cur_arg->T_id;
    int first = thread_num;
    int second = thread_num+1;
    if (second == cur_arg->tot_act){
        second = 0;
    }
    

    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[first].id);
    pthread_mutex_lock(&(cur_arg->account_ary[first].lock));
    printf("Thread[%d] Succesfully Locked account %c\n",thread_num+1,cur_arg->account_ary[first].id);
    sleep(1);

    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[second].id);
    pthread_mutex_lock(&(cur_arg->account_ary[second].lock));
    printf("Thread[%d] locked accounts %c and %c Performing transaction now\n",thread_num+1,cur_arg->account_ary[second].id,cur_arg->account_ary[second].id );
    
    switch(thread_num){

        case 0:
            cur_arg->account_ary[first].balance = cur_arg->account_ary[first].balance + 200;
            cur_arg->account_ary[second].balance = cur_arg->account_ary[second].balance - 200;
            break;
        case 1:
            //cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 200;
            cur_arg->account_ary[second].balance = cur_arg->account_ary[second].balance - 100;
            break;
        case 2:
            cur_arg->account_ary[first].balance = cur_arg->account_ary[first].balance + 150;
            cur_arg->account_ary[second].balance = cur_arg->account_ary[second].balance - 150;
            break;
        case 3:
            cur_arg->account_ary[second].balance = cur_arg->account_ary[second].balance + 50;
            //cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 200;
            break;
        default:
            cur_arg->account_ary[first].balance = cur_arg->account_ary[first].balance + thread_num;
            cur_arg->account_ary[second].balance = cur_arg->account_ary[second].balance - thread_num;
            break;
    }


    printf("Thread[%d] Transfer Complete unlocking accounts %c and %c\n",thread_num+1,cur_arg->account_ary[first].id,cur_arg->account_ary[second].id );
    pthread_mutex_unlock(&(cur_arg->account_ary[first].lock));
    pthread_mutex_unlock(&(cur_arg->account_ary[second].lock));

    
    
    return 0;
}

//sleeps while cycle detect is running
void* Phase3(void* arg){
    sleep(1);
    Phase1(arg);
    return NULL;
}


void* Phase4(void* args){

    ThreadArg *cur_arg = (ThreadArg*)args;
    int thread_num = cur_arg->T_id;
    int *act_list = cur_arg->req_act;
    BankerNeeds(thread_num, act_list[0],act_list[1]);
    sleep(2);

    BankerPermission(thread_num);
    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id);
    pthread_mutex_lock(&(cur_arg->account_ary[act_list[0]].lock));
    printf("Thread[%d] Succesfully Locked account %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id);
    

    printf("Thread[%d] Attempting to lock account %c\n",thread_num+1,cur_arg->account_ary[act_list[1]].id);
    pthread_mutex_lock(&(cur_arg->account_ary[act_list[1]].lock));
    printf("Thread[%d] locked accounts %c and %c Performing Transaction\n",thread_num+1,cur_arg->account_ary[act_list[0]].id,cur_arg->account_ary[act_list[1]].id );
    
    sleep(1);

    switch(thread_num){

        case 0:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 200;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 200;
            break;
        case 1:
            //cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 200;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 100;
            break;
        case 2:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 150;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 150;
            break;
        case 3:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + 50;
            //cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - 200;
            break;
        default:
            cur_arg->account_ary[act_list[0]].balance = cur_arg->account_ary[act_list[0]].balance + thread_num;
            cur_arg->account_ary[act_list[1]].balance = cur_arg->account_ary[act_list[1]].balance - thread_num;
            break;
    }


    printf("Thread[%d] Transaction Complete unlocking accounts %c and %c\n",thread_num+1,cur_arg->account_ary[act_list[0]].id,cur_arg->account_ary[act_list[1]].id );
    pthread_mutex_unlock(&(cur_arg->account_ary[act_list[1]].lock));
    pthread_mutex_unlock(&(cur_arg->account_ary[act_list[0]].lock));
    BankerReturn(thread_num);

    return NULL;
}



void main(){

    int phase;

    phase = 10;

    printf("Input the Scenario\n");
    printf("1 for Naive Deadlock, 2 for 2PL, 3 for Deadlock detection, 4 for Bankers\n");
    if(scanf("%d", &phase) != 1){
        while(getchar() != '\n'){
        continue;
        }
    }
    while( phase > 4 || phase < 1){
        printf("Incorrect Input try again\n");
        if(scanf("%d", &phase) != 1){
            while(getchar() != '\n'){
                continue;
            }
        
        }
    }



    int thread_count = 20;
    printf("Input Thread Count (1 Min, 10 Max)\n");
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

    






    pthread_t thread_arr[thread_count];

    int* cycle_array[thread_count];



    // Cycle Detect
    pthread_t cycle_thread;
    CycleArg CycArg;
    CycArg.thread_ar = thread_arr;
    CycArg.cycle_ar = cycle_array;
    int cyc_special[2] = {thread_count-1,0};
    CycArg.tot_thread = thread_count;
    //

    Account act_ary[thread_count];
    char id_ary[thread_count];
    int bal_ary[thread_count];
    char start_id = 'A';
    for(int i = 0; i < thread_count; i++){
        id_ary[i] = start_id;
        bal_ary[i] = (i+1)*200;
        start_id++;
    }

    bal_ary[0] = 1000;
    bal_ary[1] = 500;
    bal_ary[2] = 750;
    bal_ary[3] = 1200;


    


    ThreadArg arg_ary[thread_count]; 
    
    for(int i = 0; i < thread_count; i++){

        act_ary[i].id = id_ary[i];
        act_ary[i].balance = bal_ary[i];
        
        pthread_mutex_init(&act_ary[i].lock,NULL);
    }

    
    printf("Initial Account Values\n");
    for(int i = 0; i < thread_count; i++){
        printf("Accound ID: %c, Balance: %d\n", act_ary[i].id, act_ary[i].balance);
        
    }


    BankerInit(thread_count);
    
    for(int i = 0; i < thread_count; i++){
        
        arg_ary[i].account_ary = act_ary;
        arg_ary[i].T_id = i;
        arg_ary[i].tot_act = thread_count;
        int* req_acnt = malloc(2*sizeof(int));
        if(i == thread_count-1){
            
            req_acnt[0] = 0;
            req_acnt[1] = i;
            cycle_array[i] = cyc_special;
            
        }else{
            req_acnt[0] = i;
            req_acnt[1] = i+1;
            cycle_array[i] = req_acnt;
        }
        
        arg_ary[i].req_act = req_acnt;
        switch(phase){
            case 1:
                pthread_create(&thread_arr[i],NULL,Phase1,(void*)&arg_ary[i]);
                break;
            case 2:
                pthread_create(&thread_arr[i],NULL,Phase2,(void*)&arg_ary[i]);
                break;

            case 3:
                pthread_create(&thread_arr[i],NULL,Phase3,(void*)&arg_ary[i]);
                if(i == thread_count-1){

                    pthread_create(&cycle_thread,NULL,CycleDetect,(void*)&CycArg);
                }
                break;
            case 4:
                pthread_create(&thread_arr[i],NULL,Phase4,(void*)&arg_ary[i]);
                break;
            defalt:
                break;
        }
    }

    

    sleep(10);
    printf("Final Account Values\n");
    for(int i = 0; i < thread_count; i++){
        printf("Accound ID: %c, Balance: %d\n", act_ary[i].id, act_ary[i].balance);
        
    }

    for(int i = 0; i < thread_count; i++){
        //printf("Accound ID: %c, Balance: %d\n", act_ary[i].id, act_ary[i].balance);
        pthread_cancel(thread_arr[i]);
        pthread_join(thread_arr[i],NULL);
    }
    for(int i = 0; i < thread_count; i++){
        pthread_mutex_destroy(&act_ary[i].lock);
        free(arg_ary[i].req_act);
    }
    return;
}