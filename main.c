#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#define NUM_FILES 84

/* Switch threading mode ./main single 1 (runs first task of 4)	./main multi*/
enum threading_mode {SINGLE_THERAD, MULTI_THREAD};
enum priority_flag {LOW = 0, HIGH = 1};
	/* 	
		Switch scheduling policy 
		SCHED_OTHER, SCHED_RR, SCHED_FIFO
	*/

void *runner1(void *param);
void *runner2(void *param);
void *runner3(void *param);

pthread_mutex_t lock;

int main(int argc, char *argv[]){
	pthread_t task_tid[3];
	pthread_attr_t attr;
	int policy, single_thread_task, num_tasks, priority_task;
	threading_mode mode;
	priority_flag priority;

	/* TODO: Grant root privileges for policy and sched change */

	pthread_mutex_init (&lock, NULL);

	/* Parse user input for program options*/
	if(!strcmp(argv[1], "single"))
	{
		mode = SINGLE_THREAD;
		single_thread_task = atoi(argv[2]);
	}
	else if (!strcmp(argv[1], "multi")){
		mode = MULTI_THREAD;
		if (!strcmp(argv[2], "sched")){
			if(!strcmp(argv[3], "RR"))
				policy = SCHED_RR;
			else if(!strcmp(argv[3], "FIFO"))
				policy = SCHED_FIFO;
		}
		else if(!strcmp(argv[2], "priority")){
			priority_task = atoi(argv[3]);
			if(!strcmp(argv[4], "low"))
				priority = LOW;
			else if(!strcmp(argv[4], "high"))
				priority = HIGH;
		}
	}


	if (pthread_attr_getschedpolicy(&attr, &policy) != 0)
		fprintf(stderr, "Unable to get policy.\n");

	if (pthread_attr_setschedpolicy(&attr, policy) != 0)
			fprintf(stderr, "Unable to set policy.\n");

	/* create the threads */
	pthread_create(&task_tid[0],&attr,runner1,NULL);
	pthread_create(&task_tid[1],&attr,runner2,NULL);
	pthread_create(&task_tid[2],&attr,runner3,NULL);
	/* join threads */
	for (i = 0; i < NUM_THREADS; i++)
		pthread_join(task_tid[i], NULL);

	/* Print out results only when a Task finishes for all 84 files
	   Print in order of completion, but do not interrupt any task
	   that is currently printing a report. In other words, print entire
	   reports in the order they were finished. Do not interleave results. 

	   Ongoing tasks should not be blocked by reporting tasks.
	   */	

	printf("=== All Tasks Completed ===\n");

	pthread_mutex_destroy(&lock);
	return 0;
}

/*
	T1: Find total number of unique words among all non-numeric string literals in each file. 
	    - String literals case sensitive
*/
void *runner1(void *param)
{
	int elapsedTime;
	/* do some work ... */

	printf("=== T1 Completed ===\n");

	/* do the reporting */
	pthread_mutex_lock(&lock); 
	printf("=== T1 Report Start ===\n");


	printf("T1 RESULT: Total elapsed time: %d seconds\n", elapsedTime);
	printf("=== T1 Report End ===\n");
	pthread_mutex_unlock(&lock);
	pthread_exit(0);
}
/*
	T2: Find maximum, minimum, average, and variance of lengths of 
	    alphanumeric strings in each file, exclude pure numbers 
	    (either integer or floating-point values). 
	    - abcd1234 (8), 42(-), f22(3)
*/
void *runner2(void *param)
{
	int elapsedTime;
	/* do some work ... */

	printf("=== T2 Completed ===\n");

	/* do the reporting */
	pthread_mutex_lock(&lock); 
	printf("=== T2 Report Start ===\n");

	printf("=== T2 Report End ===\n");
	pthread_mutex_unlock(&lock);
	pthread_exit(0);
}

/*
	T3: Count ratio of missing or zero values in each file. 
	    Also, find maximum, minimum, average of the numbers of rows and columns of 
	    all files. 
*/
void *runner3(void *param)
{
	int elapsedTime;
	/* do some work ... */

	printf("=== T3 Completed ===\n");

	/* do the reporting */
	pthread_mutex_lock(&lock); 
	printf("=== T3 Report Start ===\n");

	printf("=== T3 Report End ===\n");
	pthread_mutex_unlock(&lock);
	pthread_exit(0);
}