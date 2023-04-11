#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

// In this header file, some extra macros, variables and function prototypes are define
// as there used in bench.c, kiwi.c and db.c files

// The number of threads that the system uses
// Its use is in bench.c file to distribute the requests on the threads
// and execute the multithreaded reading-writing proccess
#define NUMBER_OF_THREADS (250)
#define DATAS ("testdb")

#define KSIZE (16)
#define VSIZE (1000)

#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"

// The write percentage that the user gives as an
// input arguement
// It is used in bench.c file
extern float write_percentage;

// The total number of write requests that
// the writer threads will perform
// It is used in bench.c file
extern int writes_per_thread;
extern int reads_per_thread;

// The total writing execution time
// for the writer threads to perform all
// write requests
// It is initialized in bench.c and kiwi.c files and incremented in kiwi.c file
extern double total_write_cost;

// The total reading execution time
// for the threads to perform all
// read requests
// It is used in bench.c file
// It is initialized in bench.c and kiwi.c files and incremented in kiwi.c file
extern double total_read_cost;

long long get_ustime_sec(void);
void _random_key(char *key,int length);

// The function that is used by the writer threads to perform
// the multithreaded writing proccess
// its argument is NULL, which is passed on pthread_create() system call
// It is used in bench.c file
void * thread_write_func(void * arg);

// The function that is used by the reader threads to perform
// the multithreaded reading proccess
// its argument is NULL, which is passed on pthread_create() system call
// It is used in bench.c file
void * thread_read_func(void * arg);

// The function that initiates the read-write operation
// It is used in bench.c file
int execute_reads_writes();

// The function that initiates the read operation
// It is used in bench.c file
int execute_reads_only();

// The function that initiates the write operation
// It is used in bench.c file
int execute_writes_only();

// The function that initiates the respective operation (read,write,read-write)
// depending on the mode that the user gives as an input
// its argument is the write percentage that the user gives as input
// and the mode that the user will run (read,write,read-write)
// It is used in bench.c file
int initialise_threads(int write_percentage);
