/* using CSV parser library from https://github.com/semitrivial/csv_parser 
you can find some documentation there

after compiling with make, you can run this with ./myparsertest AIDS.csv
or with any of the csv files
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "../csv.h"

int main(int argc,char** argv) 
{

	int err, done, i = 0;
	char *csvfile = malloc(50*sizeof(char));
	strcat(csvfile,"tests/analcatdata/");
	strcat(csvfile,argv[1]);
	//printf("%s\n",csvfile);

	FILE *fp = fopen(csvfile, "r"); // CSV file we are reading from

	while (done == 0) // while not end of file
	{

 		char *line = fread_csv_line(fp, 1024, &done, &err); // reads a line from the CSV file
  		//printf("%s\n",line);

  		char **parsed = parse_csv(line); // array of strings from the current line of the CSV file

  		i = 0;
  		
  		while (parsed[i] != NULL) // prints out the parsed strings
  		{
  			printf("%s\n",parsed[i]);

  			i++;
  		} 
  		printf("\n");
	}

  	free(csvfile);
 	return 0; 
}