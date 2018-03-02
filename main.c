#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "csv_parser/csv.h"
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
int parseCSV(char *csvfile, int task, char **parsed, FILE *fp);
void getCSVfilenames();
void analyzeCSV(char **parsed_input, int task);


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
	int done = 0;
	int task = 1;
	char * filename;
	for(int i = 0; i<3; ++i){
		char *csvfile = malloc(60*sizeof(char));
		char **parsed;
		//printf("%s\n", analcatdata_filenames[i]);
		//filename = "AIDS.csv";
		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[i]);
		//printf("%s\n",csvfile);
		FILE *fp = fopen(csvfile, "r"); // CSV file we are reading from
		while(done == 0){
			done = parseCSV(csvfile, task, parsed, fp);
			/* Analyze the current parsed CSV line */

		}
		free(csvfile);
		fclose(fp);
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
	/* do some work ... */
	//printf("Thread 2\n");
	//parseCSV("AIDS.csv");
	int done = 0;
	int task = 2;
	char * filename;
	//for(int i = 0; i<NUM_FILES; i++){
		char *csvfile = malloc(50*sizeof(char));
		char **parsed;
		//filename = analcatdata_filenames[i];
		filename = "AIDS.csv";
		strcat(csvfile,"analcatdata/");
		strcat(csvfile,filename);
		//printf("%s\n",csvfile);
		// FILE *fp = fopen(csvfile, "r"); // CSV file we are reading from
		// while(done == 0){
		// 	done = parseCSV(csvfile, task, parsed, fp);
		// 	/* Analyze the current parsed CSV line */

		// }
		free(csvfile);
		// fclose(fp);
	//}
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
	/* do some work ... */
	//printf("Thread 3\n");
	//parseCSV("challenger.csv");
	int task = 3;
	int done = 0;
	char * filename;
	//for(int i = 0; i<NUM_FILES; i++){
		char *csvfile = malloc(50*sizeof(char));
		char **parsed;
		//filename = analcatdata_filenames[i];
		filename = "AIDS.csv";
		strcat(csvfile,"analcatdata/");
		strcat(csvfile,filename);
		//printf("%s\n",csvfile);
		// FILE *fp = fopen(csvfile, "r"); // CSV file we are reading from
		// while(done == 0){
		// 	done = parseCSV(csvfile, task, parsed, fp);
		// 	/* Analyze the current parsed CSV line */

		// }
		free(csvfile);
		// fclose(fp);
	//}
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

int parseCSV(char *csvfile, int task, char **parsed, FILE *fp){
	int err, done, i = 0;
	char *line = fread_csv_line(fp, 1024, &done, &err); // reads a line from the CSV file
	//printf("%s\n",line);
	parsed = parse_csv(line); // array of strings from the current line of the CSV file
	i = 0;	
	while (parsed[i] != NULL) // prints out the parsed strings
	{
		printf("%s\n",parsed[i]);
		i++;
	} 
	//printf("Task: %d\n", task);
	//printf("\n");
	return done;  	
}

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