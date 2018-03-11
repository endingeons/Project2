#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "CsvParser/include/csvparser.h"
#include <sys/types.h>
#include <dirent.h>
#include <ctype.h>
#include <sched.h>

#define NUM_FILES 84
#define NUM_THREADS 3
#define bool int
#define true 1
#define false 0

/* Switch threading mode ./main single 1 (runs first task of 4)	./main multi*/
typedef enum {SINGLE_THREAD, MULTI_THREAD} threading_mode_t;
typedef enum {LOW = 0, HIGH = 1} priority_flag_t;
	/* 	
		Switch scheduling policy 
		SCHED_OTHER, SCHED_RR, SCHED_FIFO
	*/

void *runner1(void *param);
void *runner2(void *param);
void *runner3(void *param);

void getCSVfilenames();

FILE *fp_output;
pthread_mutex_t lock, write_lock;
char * analcatdata_filenames[NUM_FILES];
const char * output_filepath = "./output.txt";
typedef struct uniqueWord
{
	char* word;
	struct uniqueWord* next;
} uniqueWord;

clock_t begin;

int main(int argc, char *argv[]){
	begin = clock();
	pthread_t tid_task[NUM_THREADS];
	pthread_attr_t attr;
	int policy, single_thread_task, num_tasks, priority_task,prio;
	threading_mode_t mode;
	priority_flag_t priority;
	struct sched_param schedparam;
	/* array of functions invoked by different threads*/
	void *(f[])  = {runner1, runner2, runner3};
	/* get the default attributes */
	pthread_attr_init(&attr);
	pthread_mutex_init (&lock, NULL);
	pthread_mutex_init (&write_lock, NULL);
	getCSVfilenames();

	pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);

	/* Open output.txt file and clear its leftover contents */
	char buffer [50];
	fp_output = fopen(output_filepath,"w");
	//sprintf(buffer, "== main.c report output ==\n\n");
	//fputs(buffer, fp_output);
	fclose(fp_output);	

	if (pthread_attr_getschedpolicy(&attr, &policy) != 0)
		fprintf(stderr, "Unable to get policy.\n");

	/* Parse user input for program options by 'multi' or 'single' parameter */
	if (argc < 2){
		printf("***ERROR: Please select multi or single threaded mode.\n");
		exit(1);
	}
	if(!strcmp(argv[1], "single") && argc == 3)
	{
		mode = SINGLE_THREAD;
		single_thread_task = atoi(argv[2]);
		if(single_thread_task < 1 || single_thread_task > 3){
			printf("***ERROR: Please choose task 1, 2, or 3 for single threading mode.");
			exit(1);
		}

	}
	else if (!strcmp(argv[1], "multi") && argc == 2)
	{
		mode = MULTI_THREAD;
	}
	else if(!strcmp(argv[1], "multi") && ( (argc == 4) || (argc == 5) ) ) /* schedule or priority change */
	{
		mode = MULTI_THREAD;
		if (!strcmp(argv[2], "sched")){
			if(!strcmp(argv[3], "RR"))
				policy = SCHED_RR;
			else if(!strcmp(argv[3], "FIFO"))
				policy = SCHED_FIFO;
			
			else{
				printf("***ERROR: Invalid scheduling algorithm. Please select RR or FIFO.\n");
				exit(1);
			}			
		}
		else if(!strcmp(argv[2], "priority")){
			priority_task = atoi(argv[3]);
			if(priority_task < 4 && priority_task > 0){
				if(!strcmp(argv[4], "low"))
					priority = LOW;
				else if(!strcmp(argv[4], "high"))
					priority = HIGH;	
			}
			else{
				printf("***ERROR: Invalid priority option. \n\t - \'priority [task] [high|low]\'\n");
				exit(1);
			}
		}
		else
		{
			printf("***ERROR: Invalid parameter.\n");
			exit(1);
		}
	}
	else if(!strcmp(argv[1], "multi") && (argc > 2))
	{
		printf("***ERROR: Valid options for multithreading are\n\t- \'priority [task] [high|low]\'\n\t- \'sched [RR|FIFO]\'\n");
		exit(1);
	}
	else{
		printf("***ERROR: Please select a mode for running (single or multi threaded).\n");
		exit(1);
	}

	/* How many/which threads should run? */
	if(mode == SINGLE_THREAD){
		pthread_create(&tid_task[0],&attr,f[single_thread_task - 1], NULL);
		pthread_join(tid_task[0], NULL);
	}
	else if(mode == MULTI_THREAD){
		if((policy == SCHED_RR) || (policy == SCHED_FIFO))
		{
			if (pthread_attr_setschedpolicy(&attr, policy) != 0)
				fprintf(stderr, "Unable to set policy.\n");
		}

		if ( (argc == 2) || (argc == 4) )
		{
			schedparam.sched_priority = 50;
			pthread_attr_setschedparam(&attr, &schedparam);

			pthread_create(&tid_task[0],&attr,runner1, NULL);
			pthread_create(&tid_task[2],&attr,runner3, NULL);
			pthread_create(&tid_task[1],&attr,runner2, NULL);

			pthread_join(tid_task[0],NULL);
			pthread_join(tid_task[2],NULL);
			pthread_join(tid_task[1],NULL);	
		}
		else if(priority == LOW)
		{
			/* We cannot directly change SCHED_OTHER priority so we must set as FIFO or RR realtime scheduling */
			pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
			schedparam.sched_priority = 1;
			pthread_attr_setschedparam(&attr, &schedparam);
			if (!strcmp(argv[3],"1")) /* gives error when we give priority to any task other than 1 */
			{
				schedparam.sched_priority = 99;
				pthread_attr_setschedparam(&attr, &schedparam);
				pthread_create(&tid_task[0],&attr,runner1, NULL);
				pthread_create(&tid_task[1],&attr,runner2, NULL);
				pthread_create(&tid_task[2],&attr,runner3, NULL);

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[2],NULL);
				pthread_join(tid_task[1],NULL);		
			}
			else if (!strcmp(argv[3],"2"))
			{
				pthread_create(&tid_task[0],&attr,runner2, NULL);
				schedparam.sched_priority = 99;
				pthread_attr_setschedparam(&attr, &schedparam);
				pthread_create(&tid_task[1],&attr,runner1, NULL);
				pthread_create(&tid_task[2],&attr,runner3, NULL);

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[1],NULL);
				pthread_join(tid_task[2],NULL);				
			}
			else if (!strcmp(argv[3],"3"))
			{
				pthread_create(&tid_task[0],&attr,runner3, NULL);
				schedparam.sched_priority = 99;
				pthread_attr_setschedparam(&attr, &schedparam);
				pthread_create(&tid_task[1],&attr,runner1, NULL);
				pthread_create(&tid_task[2],&attr,runner2, NULL);	

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[1],NULL);
				pthread_join(tid_task[2],NULL);			
			}

		}
		else if(priority == HIGH)
		{
			pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
			schedparam.sched_priority = 99;
			pthread_attr_setschedparam(&attr, &schedparam);

			if (!strcmp(argv[3],"1"))
			{
				pthread_create(&tid_task[0],&attr,runner1, NULL);
				schedparam.sched_priority = 1;
				pthread_attr_setschedparam(&attr, &schedparam);
				pthread_create(&tid_task[1],&attr,runner2, NULL);
				pthread_create(&tid_task[2],&attr,runner3, NULL);

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[2],NULL);
				pthread_join(tid_task[1],NULL);	
			}
			else if (!strcmp(argv[3],"2"))
			{
				pthread_create(&tid_task[0],&attr,runner1, NULL);
				pthread_create(&tid_task[1],&attr,runner2, NULL);
				pthread_create(&tid_task[2],&attr,runner3, NULL);	

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[2],NULL);
				pthread_join(tid_task[1],NULL);					
			}
			else if (!strcmp(argv[3],"3"))
			{
				pthread_create(&tid_task[0],&attr,runner1, NULL);
				pthread_create(&tid_task[1],&attr,runner2, NULL);
				pthread_create(&tid_task[2],&attr,runner3, NULL);	

				pthread_join(tid_task[0],NULL);
				pthread_join(tid_task[2],NULL);	
				pthread_join(tid_task[1],NULL);			
			}				

		}

	}	
	else{
		printf("***ERROR: Invalid mode selected).\n");
		exit(1);
	}	

	/* Print out results only when a Task finishes for all 84 files
	   Print in order of completion, but do not interrupt any task
	   that is currently printing a report. In other words, print entire
	   reports in the order they were finished. Do not interleave results. 

	   Ongoing tasks should not be blocked by reporting tasks.
	   */	
	clock_t end = clock();
	double elapsed = (double)(end - begin) / CLOCKS_PER_SEC;

	fp_output = fopen(output_filepath,"ab");
	sprintf(buffer, "=== All Tasks Completed ===\n");
	fputs(buffer, fp_output);

	sprintf(buffer, "=== All Tasks Report Start ===\n");
	fputs(buffer, fp_output);

	sprintf(buffer,"RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	fputs(buffer, fp_output);

	sprintf(buffer,"=== All Tasks Report End ===\n");
	fputs(buffer, fp_output);

	fflush(fp_output);
	fclose(fp_output);	
	buffer[0] = '\0';

	printf("=== All Tasks Completed ===\n");
	printf("=== All Tasks Report Start ===\n");
	printf("RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	printf("=== All Tasks Report End ===\n");

	pthread_mutex_destroy(&lock);
	pthread_mutex_destroy(&write_lock);
	pthread_attr_destroy(&attr);
	return 0;
}

/*
	T1: Find total number of unique words among all non-numeric string literals in each file. 
	    - String literals case sensitive
*/
bool isNonNumeric(char* word)
{
	int i;
	for (i = 0; i < strlen(word); i++)
	{
		if (isdigit(word[i]) > 0) 
		{
			return false;
		}
	}
	return true;
}

void printUniqueWords(uniqueWord* list,char* filename)
{
	printf("\n ~Printing out unique words in %s~\n",filename);
	while (list != NULL)
	{
		printf("%s\n",list->word);
		list = list->next;
	}
}

bool checkUnique(char* word,uniqueWord* list)
{

	if (list == NULL)
	{
		return true;
	}

	do
	{
		if (strcmp(word,list->word) == 0)
		{
			return false;
		}

		list = list->next;

	} while (list != NULL);

	return true;
}

void freeList(uniqueWord* list)
{
	uniqueWord* temp;

	while (list != NULL)
	{
		temp = list;
		list = list->next;
		pthread_mutex_lock(&lock); 
		free(temp);
		pthread_mutex_unlock(&lock); 
	}
}

void *runner1(void *param)
{
	char buffer[1000];
	int uniquesPerFile[NUM_FILES] = {0};

	for(int i = 0; i < NUM_FILES; ++i)
	{
		char *csvfile = malloc(100*sizeof(char));

		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[i]);

    	int j;
    /*                                   file, delimiter, first_line_is_header? */
  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
    	CsvRow *row;
    	uniqueWord* wordHead = (uniqueWord*)malloc(sizeof(uniqueWord));
		uniqueWord* wordTail = (uniqueWord*)malloc(sizeof(uniqueWord));

		wordHead = NULL;

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	const char **rowFields = CsvParser_getFields(row);

			/* At this point we have the fields from the CSV file parsed. 
			Now we parse the words and count the unique ones */

			int a,b;
			for (a = 0;a < CsvParser_getNumFields(row);a++)
			{
				const char* field = rowFields[a];
				b = 0;
				
				char* fieldCopy = malloc(100*sizeof(char));
				int c = 0;
				while (field[c] != 0)
				{
					fieldCopy[c] = field[c];
					c++;
				}

				char* currentWord = &fieldCopy[0];
				
				while(fieldCopy[b] != '\0') /* null character*/
				{
					
					if (fieldCopy[b] == ' ') /* space */
					{
						fieldCopy[b] = '\0';
						
						/* First, check if the string has no numbers */
						if (isNonNumeric(currentWord))
						{
							/* Then, check if it's unique */


							if (checkUnique(currentWord,wordHead))
							{
								/* add the word to the list */
								uniqueWord* new_node = (uniqueWord*)malloc(sizeof(uniqueWord));
								new_node->word = currentWord;
								new_node->next = NULL;

								if (wordHead == NULL)
								{
									/*Adding the head */
									wordHead = new_node;	
								}
								if (wordTail != NULL)
								{
									wordTail->next = new_node;
								}
								wordTail = new_node;
								uniquesPerFile[i]++;
							} 

						}
						currentWord = &fieldCopy[b+1]; /* Move on to the next word */	
					}
					b++;					
				} 				
				if (isNonNumeric(currentWord))
				{
					/* non numeric */
					if (checkUnique(currentWord,wordHead))
					{
						/* is unique */
						/* add the word to the list */
						uniqueWord* new_node = (uniqueWord*)malloc(sizeof(uniqueWord));
						new_node->word = currentWord;
						new_node->next = NULL;

						if (wordHead == NULL)
						{
							/* Added the head */
							wordHead = new_node;	
						}
						if (wordTail != NULL)
						{
							wordTail->next = new_node;
						}
						wordTail = new_node;
						if (strcmp("",currentWord) != 0)
						{
							uniquesPerFile[i]++;
						}
						
						
					} 
				}



			}

        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);		
    	// print out the list of unique words
		//printUniqueWords(wordHead,csvfile);	
		freeList(wordHead);
		free(csvfile);

	}

	fp_output = fopen(output_filepath,"ab");
	sprintf(buffer, "=== T1 Completed ===\n");
	fputs(buffer, fp_output);
	fflush(fp_output);
	fclose(fp_output);	
	buffer[0] = '\0';
	printf("=== T1 Completed ===\n");

	/* do the reporting */
	pthread_mutex_lock(&write_lock); 
	/* Print to console */
	printf("=== T1 Report Start ===\n");

	/* Print to output.txt*/
	fflush(fp_output);
	fp_output = fopen(output_filepath,"ab");
	fputs("=== T1 Report Start ===\n", fp_output);
	int i;
	for (i = 0; i < NUM_FILES; i++)
	{
		sprintf(buffer, "T1 RESULT: File %s: Total number of unique words: %d\n",analcatdata_filenames[i],uniquesPerFile[i]);
		fputs(buffer,fp_output);
	}
	clock_t time1 = clock();
	double elapsed = (double)(time1 - begin) / CLOCKS_PER_SEC;
	/* Print to console */
	printf("T1 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	printf("=== T1 Report End ===\n");

	/*Print to output.txt*/
	buffer[0] = '\0';
	sprintf(buffer, "T1 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	fputs(buffer, fp_output);	
	buffer[0] = '\0';
	sprintf(buffer, "=== T1 Report End ===\n");
	fputs(buffer, fp_output);
	fclose(fp_output);
	pthread_mutex_unlock(&write_lock); 
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
	int min[NUM_FILES];
	int max[NUM_FILES];
	const int MAX_STRING_LEN = 3000;
	float avg[NUM_FILES] = {0};
	float temp = 0;
	float variance[NUM_FILES] = {0};
	float exp_sos;
	char * filename;
	char c;
	char buffer[100];

	for(int file_idx = 0; file_idx<NUM_FILES; ++file_idx)
	{	
		float totalNumStrings = 0;
		int alphanumeric_lengths[MAX_STRING_LEN]; /* from analyzing the csv files */
		char *csvfile = malloc(50*sizeof(char));

		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[file_idx]);
    	int j;
    /*                                   file, delimiter, first_line_is_header? */
  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
    	CsvRow *row;

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	const char **rowFields = CsvParser_getFields(row);
        	for (j = 0 ; j < CsvParser_getNumFields(row) ; j++) 
       		{
       			for(int k = 0; k < strlen(rowFields[j]); k++){
		   			c = rowFields[j][k];
		   			//printf("%c", c);
		   			if((isalpha(c) || c == '-')&& c != '\0' ){
		   				/* Increment the total alphanumeric strings that were found */
		   				totalNumStrings++;
						alphanumeric_lengths[j] = strlen(rowFields[j]);
						break;
					}
					else{ /* Pure number, DO NOTHING */
					}
				}
       		}
        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);
    	free(csvfile);

    	/* Maximum, minimum, average, and variance */
    	min[file_idx] = alphanumeric_lengths[0];
    	max[file_idx] = alphanumeric_lengths[0];
    	for(int i = 0; i<totalNumStrings; i++){
    		if(min[file_idx] > alphanumeric_lengths[i]) min[file_idx] = alphanumeric_lengths[i];
    		if(max[file_idx] < alphanumeric_lengths[i]) max[file_idx] = alphanumeric_lengths[i];
    		avg[file_idx] += alphanumeric_lengths[0];
    	}
    	avg[file_idx] /= (float)totalNumStrings;
    	
    	/* Compute the squared deviations */
    	for(int i = 0; i<totalNumStrings; i++){
    		exp_sos += (alphanumeric_lengths[i] - avg[file_idx]) * (alphanumeric_lengths[i] - avg[file_idx]);
    	}
    	exp_sos /= (totalNumStrings - 1);
    	variance[file_idx] = exp_sos;
	}
	/* print to console */
	printf("=== T2 Completed ===\n");
	/* print to output.txt */
	fp_output = fopen(output_filepath,"ab");
	sprintf(buffer, "=== T2 Completed ===\n");
	fputs(buffer, fp_output);
	fflush(fp_output);
	fclose(fp_output);	
	buffer[0] = '\0';
	/* do the reporting */
	pthread_mutex_lock(&write_lock); 
	fflush(fp_output);
	fp_output = fopen(output_filepath,"ab");
	fputs("=== T2 Report Start ===\n", fp_output);
	/* Print to console */
	printf("=== T2 Report Start ===\n");
	int i;
	/* Print to output.txt */
	for (i = 0; i < NUM_FILES; i++)
	{
		sprintf(buffer,"T2 RESULT: File %s : Max = %d, Min = %d, Avg = %.3f, Var = %.3f\n", 
					analcatdata_filenames[i], max[i], min[i], avg[i], variance[i]);
		fputs(buffer,fp_output);
	}

	clock_t time2 = clock();
	double elapsed = (double)(time2 - begin) / CLOCKS_PER_SEC;
	/* print console */
	printf("T2 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	printf("=== T2 Report End ===\n");
	/* print output.txt */	
	buffer[0] = '\0';
	sprintf(buffer, "T2 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	fputs(buffer, fp_output);
	buffer[0] = '\0';
	sprintf(buffer, "=== T2 Report End ===\n");
	fputs(buffer, fp_output);
	fclose(fp_output);
	pthread_mutex_unlock(&write_lock); 
	pthread_exit(0);
}

/*
	T3: Count ratio of missing or zero values in each file. 
	    Also, find maximum, minimum, average of the numbers of rows and columns of 
	    all files. 
*/

int getNumRows(char* filename)
{
  	CsvParser *csvparser = CsvParser_new(filename, ",", 0);
    CsvRow *row;	
    int num;

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	num++;
        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);	

    	return num;

}

void *runner3(void *param)
{
	double* ratioPerFile = malloc(NUM_FILES*sizeof(double));
	int* rowsPerFile = malloc(NUM_FILES*sizeof(int));
	int* colsPerFile = malloc(NUM_FILES*sizeof(int));
	char buffer[100];
	int rowcount;
	int numMissing;

	for(int i = 0; i<NUM_FILES; ++i)
	{
		rowcount = 0;
		numMissing = 0;
		char *csvfile = malloc(100*sizeof(char));

		strcat(csvfile,"analcatdata/");
		strcat(csvfile,analcatdata_filenames[i]);


		
    	int j;
    /*                                   file, delimiter, first_line_is_header?*/
  	    CsvParser *csvparser = CsvParser_new(csvfile, ",", 0);
    	CsvRow *row;
    	int maxColumns = 0;

    	/* First, find the number of rows and the maximum number of columns.*/
   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
   			pthread_mutex_lock(&lock);
   			rowcount++;
   			rowsPerFile[i]++;
   			pthread_mutex_unlock(&lock);
        	const char **rowFields = CsvParser_getFields(row);

        	if (CsvParser_getNumFields(row) > maxColumns)
        	{
        		pthread_mutex_lock(&lock);
        		maxColumns = CsvParser_getNumFields(row);
        		colsPerFile[i] = maxColumns;
        		pthread_mutex_unlock(&lock);
        	}


        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);  		   			

    	/* Then get the number of missing or zero values. */

  	    csvparser = CsvParser_new(csvfile, ",", 0);
    	int numZero = 0;

   		while ((row = CsvParser_getRow(csvparser)) ) 
   		{
        	const char **rowFields = CsvParser_getFields(row);
        	/* check for missing fields */
        	
        	if (CsvParser_getNumFields(row) < maxColumns)
        	{
				pthread_mutex_lock(&lock);        		
        		numMissing += (maxColumns - CsvParser_getNumFields(row));
        		pthread_mutex_unlock(&lock);	
        	}
        	
        	/* check for zero values */
        	float val;
        	bool validNum;
         	for (j = 0 ; j < CsvParser_getNumFields(row) ; j++) 
       		{
            	const char* currentField = rowFields[j];
            	validNum = true;
            	/* check if all digits or '.' */
            	for (int k = 0;k < strlen(currentField);k++)
            	{
            		if (!(isdigit(currentField[k]) || (currentField[k] == '.') ))
            		{
            			validNum = false;
            		}
            	}

            	if (validNum)
            	{
            		val = atof(currentField);
            		if (val == 0)
            		{
            			pthread_mutex_lock(&lock);
            			numZero++;
            			pthread_mutex_unlock(&lock);
            		}
            	}
       		}
			      	

        	CsvParser_destroy_row(row);
  		}
    	CsvParser_destroy(csvparser);	 

		pthread_mutex_lock(&lock);
    	double ratio = ((double)numMissing + (double)numZero) / ((double)maxColumns * (double)rowcount) * 100;
    	ratioPerFile[i] = ratio;
    	pthread_mutex_unlock(&lock);
		free(csvfile);

	} 

/* Find the min, max, and average number of rows and columns in all of the files */

	int minRow = rowsPerFile[0];
	int maxRow = rowsPerFile[0];
	int minCol = colsPerFile[0];
	int maxCol = colsPerFile[0];
	int sumRows = rowsPerFile[0];
	int sumCols = colsPerFile[0];

	for (int i = 1;i < NUM_FILES;i++)
	{
		if (rowsPerFile[i] < minRow)
			minRow = rowsPerFile[i];
		if (rowsPerFile[i] > maxRow)
			maxRow = rowsPerFile[i];
		if (colsPerFile[i] < minCol)
			minCol = colsPerFile[i];
		if (colsPerFile[i] > maxCol)
			maxCol = colsPerFile[i];	

		sumRows += rowsPerFile[i];
		sumCols += colsPerFile[i];	
	}

	

	double avgRow = (double)sumRows / (double)NUM_FILES;
	double avgCol = (double)sumCols / (double)NUM_FILES;
	/* print console */
	printf("=== T3 Completed ===\n");
	/* print output.txt */
	fp_output = fopen(output_filepath,"ab");
	sprintf(buffer, "=== T3 Completed ===\n");
	fputs(buffer, fp_output);
	fflush(fp_output);
	fclose(fp_output);	
	buffer[0] = '\0';

	/* do the reporting */
	pthread_mutex_lock(&write_lock); 
	/* print console*/
	printf("=== T3 Report Start ===\n");
	/* print output.txt*/
	fflush(fp_output);
	fp_output = fopen(output_filepath,"ab");
	fputs("=== T3 Report Start ===\n", fp_output);
	int i;
	for (i = 0; i < NUM_FILES; i++)
	{
		sprintf(buffer,"T3 RESULT: File %s: Ratio = %.2f%%\n",analcatdata_filenames[i],ratioPerFile[i]);
		fputs(buffer,fp_output);
		buffer[0] = '\0';
	}

	sprintf(buffer,"T3 RESULT: Rows: Max = %d, Min = %d, Avg = %.2f\n",maxRow,minRow,avgRow);
	fputs(buffer,fp_output);
	sprintf(buffer,"T3 RESULT: Cols: Max = %d, Min = %d, Avg = %.2f\n",maxCol,minCol,avgCol);
	fputs(buffer,fp_output);

	clock_t time3 = clock();
	double elapsed = (double)(time3 - begin) / CLOCKS_PER_SEC;
	/* print console */
	printf("T3 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	printf("=== T3 Report End ===\n");
	/* print output.txt */
	buffer[0] = '\0';
	sprintf(buffer, "T3 RESULT: Total elapsed time: %.3f seconds\n",elapsed);
	fputs(buffer, fp_output);	
	buffer[0] = '\0';
	sprintf(buffer, "=== T3 Report End ===\n");
	fputs(buffer, fp_output);
	fclose(fp_output);
	pthread_mutex_unlock(&write_lock); 
	pthread_exit(0);
}

/* Initialize an array with all of the filenames in the directory*/
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
	  	size_t destination_size = sizeof(ent->d_name);
	    analcatdata_filenames[i] = (char*)malloc(sizeof(char) * destination_size);
	    strcpy(analcatdata_filenames[i], ent->d_name);	   
	    i++;
	  }
	  closedir (dir);
	} else {
	  /* could not open directory */
	  perror ("Could not open directory");
	  
	}
}