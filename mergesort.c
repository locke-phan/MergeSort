#include <stdio.h>
#include <limits.h>
#include <stdlib.h> // for malloc
#include <string.h> // for strerror
#include <math.h>

#include <pthread.h>
#include <dispatch/dispatch.h>


void mergeSort( int * , int , int );
void merge( int * , int , int , int );
void* _mergeSort_pThread( void * );
void* _merge_pThread( void * ); 

void mergeSort_dispatch( int * , int , int );
void mergeSort_pThread( int * , int , int );

typedef struct _ms_thread_data {
	int *A;
	int p;
	int q;
	int r;
} ms_thread_data;

int main (int argc, const char * argv[])
{
	int SIZE = 5000;
	int NUM_THREADS = 2;
	
	int *A = malloc(sizeof(int)*(SIZE));
	if ( A == NULL ) exit(EXIT_FAILURE);
	
	int i, j;
	j = SIZE;
	for (i = 0; i < SIZE; i++,j--)
		A[i] = j;
	
	//mergeSort_pThread( A, NUM_THREADS, SIZE );
	//mergeSort(A, 0, SIZE - 1);
	mergeSort_dispatch(A, NUM_THREADS, SIZE);
	
	if ( SIZE < 10000 )
		for(i = 0; i < SIZE; i++)
			printf("%d ", A[i]);
	
	printf("\n");
    
	free(A);
    
	return 0;
}


void mergeSort_dispatch( int *A, int NUM_BLOCK, int N ) {
	
    int size_per_block = N / NUM_BLOCK;
	
	int from = 0;
	int i, j;
    
    dispatch_queue_t global_queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    // Divide and Conquer
    dispatch_group_t merge_group = dispatch_group_create();
	for (i = 0; i < NUM_BLOCK; i++) {
		int to = (size_per_block * (i + 1))-1; 
		dispatch_group_async(merge_group, global_queue, ^{ mergeSort(A, from, to); });
		from = to + 1;
	}
    dispatch_group_wait(merge_group, DISPATCH_TIME_FOREVER);
    dispatch_release(merge_group);
    
    // Combine
    int number_of_merge_times = (int) log2(NUM_BLOCK);
	int NUMBER_OF_MERGE_BLOCK = NUM_BLOCK / 2;
	
	for (i = 0; i < number_of_merge_times; i++) {
		size_per_block = N / NUMBER_OF_MERGE_BLOCK;
        dispatch_group_t group = dispatch_group_create();
		from = 0;
		for (j = 0; j < NUMBER_OF_MERGE_BLOCK; j++) {
			
			int to = (size_per_block * (j + 1))-1; 
            dispatch_group_async(group, global_queue, ^{
                int q = (((to-from)-1)/2) + from;
                merge(A, from, q, to);
            });
			from = to + 1;
		}
        dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
        dispatch_release(group);
        NUMBER_OF_MERGE_BLOCK /= 2;
	}
}

void mergeSort( int *A , int p , int r ) {
	if ( p < r ) {
		int q = ( p + r ) / 2;
		mergeSort(A, p, q );
		mergeSort(A, q + 1, r);
		merge(A, p, q, r);
	}
}

void mergeSort_pThread( int *A, int NUM_THREADS, int N ) {
	
	pthread_t threads[NUM_THREADS];
	ms_thread_data data_per_thread[NUM_THREADS];
	
	int size_per_thread = N / NUM_THREADS;
	
	int from = 0;
	int i, j;
	for (i = 0; i < NUM_THREADS; i++) {
		
		int to = (size_per_thread * (i + 1))-1; 
		
		ms_thread_data *arg = &data_per_thread[i];
		
		arg->A = A;
		arg->p = from;
		arg->r = to;
				
		int error;
		if ( (error = pthread_create(&threads[i], NULL, 
                                     _mergeSort_pThread, arg))) {
			fprintf(stderr, "Error %d: %s.\n", error, strerror(error));
			exit(EXIT_FAILURE);
		}
		
		from = to + 1;
	}
	
	for (i = 0; i < NUM_THREADS; i++)
		pthread_join(threads[i], NULL);	
	
	// Merge phase
	int number_of_merge_times = (int) log2(NUM_THREADS);
	int NUMBER_OF_MERGE_THREADS = NUM_THREADS / 2;
	
	for (i = 0; i < number_of_merge_times; i++) {
		
		pthread_t merge_threads[NUMBER_OF_MERGE_THREADS];
		
		size_per_thread = N / NUMBER_OF_MERGE_THREADS;
		
		int error;
		from = 0;
		for (j = 0; j < NUMBER_OF_MERGE_THREADS; j++) {
			
			int to = (size_per_thread * (j + 1))-1; 
			
            ms_thread_data *arguments = malloc(sizeof(ms_thread_data));
            arguments->A = A;
			arguments->p = from;
			arguments->q = (((to-from)-1)/2) + from;
			arguments->r = to;
			            
            pthread_attr_t attributes;
            pthread_attr_init(&attributes);
            pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_JOINABLE);
			
			if ( (error = pthread_create(&merge_threads[j], &attributes, 
                                         _merge_pThread, arguments))) {
				fprintf(stderr, "Error %d: %s.\n", error, strerror(error));
				exit(EXIT_FAILURE);
			}
			
			from = to + 1;
		}
		
		int z;
		for (z = 0; z < NUMBER_OF_MERGE_THREADS; z++) {
			if ((error = pthread_join(merge_threads[z], NULL)) != 0)
				printf("Error joining threads...%s\n", strerror(error));
		}
        NUMBER_OF_MERGE_THREADS /= 2;
	}
}

void* _mergeSort_pThread( void *args ) {
	ms_thread_data *data = (ms_thread_data *) args;
    
	int *A = data->A;
	int p = data->p;
	int r = data->r;	
	
	if ( p < r ) {
		int q = ( p + r ) / 2;	
        
		ms_thread_data aThreadData;
		aThreadData.A = A;
		aThreadData.p = p;
		aThreadData.r = q;
		_mergeSort_pThread( &aThreadData );
        
		ms_thread_data bThreadData;
		bThreadData.A = A;
		bThreadData.p = q+1;
		bThreadData.r = r;
		_mergeSort_pThread( &bThreadData );
		
		merge(A, p, q, r);
	}
    return NULL;
}

void* _merge_pThread( void *args ) {
	ms_thread_data *data = (ms_thread_data *) args;
	
	int *A = data->A;
	int p = data->p;
	int q = data->q;
	int r = data->r;
	
	merge(A, p, q, r);
	free(args);
    pthread_exit(NULL);
}


void merge( int *A , int p , int q , int r ) {
	int n1 = q - p + 1;
	int n2 = r - q;
	
	int *L = malloc(sizeof(int)*(n1 + 1));
	int *R = malloc(sizeof(int)*(n2 + 1));
    
	if ( L == NULL || R == NULL ) {
		fprintf(stderr, "Error allocating memory.");
		exit(EXIT_FAILURE);	
	}
    
	int i,j;
	for ( i = 0; i <= n1 ; i++)
		L[i] = A[p + i];
    
	for ( j = 0; j <= n2 ; j++)
		R[j] = A[q + j + 1];
    
	L[n1] = INT_MAX;
	R[n2] = INT_MAX;
    
	int k;
	i = j = 0;
	for ( k = p ; k <= r ; k++) {
		if ( L[i] <= R[j] ) {
			A[k] = L[i];
			i++;
		} else {
			A[k] = R[j];
			j++;
		}
	}
	
	free(L);
	free(R);
}
