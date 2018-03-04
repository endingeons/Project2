#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "CsvParser/include/csvparser.h"
#include <sys/types.h>
#include <dirent.h>

#define NUM_FILES 84
#define NUM_THREADS 3

/* Switch threading mode ./main single 1 (runs first task of 4)	./main multi*/
enum threading_mode {SINGLE_THERAD, MULTI_THREAD};
enum priority_flag {LOW = 0, HIGH = 1};
typedef enum threading_mode threading_mode_t;
typedef enum priority_flag priority_flag_t;
	/* 	
		Switch scheduling policy 
		SCHED_OTHER, SCHED_RR, SCHED_FIFO
	*/

void *runner1(void *param);
void *runner2(void *param);
void *runner3(void *param);

void getCSVfilenames();


pthread_mutex_t lock;
char * analcatdata_filenames[NUM_FILES];

int main(int argc, char *argv[]){
	pthread_t tid_task[NUM_THREADS];
	pthread_attr_t attr;
	int policy, single_thread_task, num_tasks, priority_task;
	
	policy = SCHED_OTHER;
	threading_mode_t mode;
	priority_flag_t priority;

	/* TODO: Grant root privileges for policy and sched change */
	/* get the default attributes */
	pthread_attr_init(&attr);
	pthread_mutex_init (&lock, NULL);
	getCSVfilenames();
	// for(int i = 0; i < NUM_FILES; i++){
	// 	printf("%d\t", i+1);
	// 	printf("%s\n",analcatdata_filenames[i]);
	// }

	/* Parse user input for program options*/
	// if(!strcmp(argv[1], "single"))
	// {
	// 	mode = SINGLE_THREAD;
	// 	single_thread_task = atoi(argv[2]);
	// analcatdata_filenames[NUM_FILES];}
	// else if (!strcmp(argv[1], "multi")){
	// 	mode = MULTI_THREAD;
	// 	if (!strcmp(argv[2], "sched")){
	// 		if(!strcmp(argv[3], "RR"))
	// 			policy = SCHED_RR;
	// 		else if(!strcmp(argv[3], "FIFO"))
	// 			policy = SCHED_FIFO;
	// 	}
	// 	else if(!strcmp(argv[2], "priority")){
	// 		priority_task = atoi(argv[3]);
	// 		if(!strcmp(argv[4], "low"))
	// 			priority = LOW;
	// 		else if(!strcmp(argv[4], "high"))
	// 			priority = HIGH;
	// 	}
	// }


	if (pthread_attr_getschedpolicy(&attr, &policy) != 0)
		fprintf(stderr, "Unable to get policy.\n");

	if (pthread_attr_setschedpolicy(&attr, policy) != 0)
			fprintf(stderr, "Unable to set policy.\n");

	/* create the threads */
	pthread_create(&tid_task[0],&attr,runner1, NULL);
	pthread_create(&tid_task[1],&attr,runner2, NULL);
	pthread_create(&tid_task[2],&attr,runner3, NULL);
	/* join threads */
	for (int i = 0; i < NUM_THREADS; i++)
		pthread_join(tid_task[i], NULL);

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
	//int elapsedTime;
	/* do some work ... */
	// printf("Thread 1\n");

	char * filename;
	for(int i = 0; i<5; ++i)
	{
		char *csvfile = malloc(100*sizeof(char));

		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[i]);
		
    	int j;
    //                                   file, delimiter, first_line_is_header?
  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
    	CsvRow *row;
    	
		printf("READING FROM %s\n",csvfile);

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	const char **rowFields = CsvParser_getFields(row);
        	for (j = 0 ; j < CsvParser_getNumFields(row) ; j++) 
       		{
            	printf("%s ", rowFields[j]);
       		}
			printf("\n");
        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);			

		free(csvfile);

	}
	
	printf("=== T1 Completed ===\n");
	/* do the reporting */
	pthread_mutex_lock(&lock); 
	printf("=== T1 Report Start ===\n");


	//printf("T1 RESULT: Total elapsed time: %d seconds\n", elapsedTime);
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
	//int elapsedTime;
	/* do some work ... */
	// printf("Thread 1\n");
	size_t total_alphanumeric_strings[NUM_FILES] = {0};
	int min[NUM_FILES] = {0};
	int max[NUM_FILES] = {0};
	int avg[NUM_FILES = {0}];
	int temp = 0;
	float variance[NUM_FILES];


	char * filename;
	char c;
	for(int file_idx = 0; file_idx<NUM_FILES; ++file_idx)
	{
		char *csvfile = malloc(100*sizeof(char));

		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[file_idx]);
		
    	int j;
    //                                   file, delimiter, first_line_is_header?
  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
    	CsvRow *row;
    	
		printf("READING FROM %s\n",csvfile);

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	const char **rowFields = CsvParser_getFields(row);
        	for (j = 0 ; j < CsvParser_getNumFields(row) ; j++) 
       		{
       			for(int k = 0; j<strlen(rowFields[j][k]) - 1; ++k){
		   			c = rowFields[j][k];
		   			if(isalpha(c) && c != NULL){
						//printf("%s is not a pure number because of '%c'.\n", rowFields[j], c);
						++total_alphanumeric_strings[file_idx];
						/* If first string of file, initalize min and max */
						if(total_alphanumeric_strings[file_idx] == 1){
							max[file_idx] = strlen(rowFields[j]);
							min[file_idx] = strlen(rowFields[j]);
						}
						else{
							temp = strlen(rowFields[j]);

							 /* TODO: Find max, min, average, variance */
							if(temp > max[file_idx])
								max[file_idx] = temp;
							if(temp < min[file_idx])
								min[file_idx] = temp;
							break;
						}
					}
					else{ /* Pure number, DO NOTHING */
						printf("%s is a pure number\n", rowFields[j]);

					}
				}
            	//printf("%s ", rowFields[j]);
       		}

			//printf("\n");
        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);			

		free(csvfile);

	}

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
	//int elapsedTime;
	/* do some work ... */
	// printf("Thread 1\n");

	// char * filename;
	// for(int i = 0; i<5; ++i)
	// {
	// 	char *csvfile = malloc(100*sizeof(char));

	// 	strcat(csvfile,"analcatdata/");
	// 	strcat(csvfile,analcatdata_filenames[i]);
		
 //    	int j;
 //    //                                   file, delimiter, first_line_is_header?
 //  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
 //    	CsvRow *row;
    	
	// 	printf("READING FROM %s\n",csvfile);

 //   		while ((row = CsvParser_getRow(csvparser)) ) 
 //   		{
 //        	const char **rowFields = CsvParser_getFields(row);
 //        	for (j = 0 ; j < CsvParser_getNumFields(row) ; j++) 
 //       		{
 //            	printf("%s ", rowFields[j]);
 //       		}
	// 		printf("\n");
 //        	CsvParser_destroy_row(row);
 //  		}
 //    	CsvParser_destroy(csvparser);			

	// 	free(csvfile);

	// }
	
	printf("=== T3 Completed ===\n");
	/* do the reporting */
	pthread_mutex_lock(&lock); 
	printf("=== T3 Report Start ===\n");

	printf("=== T3 Report End ===\n");
	pthread_mutex_unlock(&lock);
	pthread_exit(0);
}

/* using CSV parser library from https://github.com/semitrivial/csv_parser 
you can find some documentation there

after compiling with make, you can run this with ./myparsertest AIDS.csv
or with any of the csv files
*/

/* Initialize an array with all of the filenames in the directory*/
/* https://stackoverflow.com/questions/612097/how-can-i-get-the-list-of-files-in-a-directory-using-c-or-c */
void getCSVfilenames(){
	DIR *dir;
	int i = 0;
	struct dirent *ent;
	if ((dir = opendir ("analcatdata")) != NULL) {
	  /* print all the files and directories within directory */
	  while ((ent = readdir (dir)) != NULL) {

	  	/* Do not copy the current and parent directories */
	  	if(!strcmp(ent ->d_name, "."))
	  		continue;
	  	if(!strcmp(ent ->d_name, ".."))
	  		continue;
	  	if(!strcmp(ent ->d_name, "README"))
	  		continue;
	  	//printf("%s\n", ent->d_name);
	  	size_t destination_size = sizeof(ent->d_name);
	    analcatdata_filenames[i] = (char*)malloc(sizeof(char) * destination_size);
	    strcpy(analcatdata_filenames[i], ent->d_name);
	    //printf("%s\n", analcatdata_filenames[i]);
	   
	    i++;
	  }
	  closedir (dir);
	} else {
	  /* could not open directory */
	  perror ("");
	  return EXIT_FAILURE;
	}

}