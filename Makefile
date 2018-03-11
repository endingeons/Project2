all:
	gcc main.c -lpthread -I ../include CsvParser/src/csvparser.c -o main
