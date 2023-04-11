// In bench.h header file, there are some additions in terms of
// functions and variables
// Extencive details are given on the report
// Right now, there will be described the way the functions
// and variables are used

#include "bench.h"

// the pthread library is used so that multithreading can be achieved
#include <pthread.h>


// the math libary is included, as the round() function is used
// in the write/read requests distribution in the threads
#include <math.h>

// In order to implement the readers - writer algorithm
// some functions from other files are required on bench.c file
// example given, the _read_test(), _read_test() functions
// and the functions that are used to open and close the DB
// (db_add() , db_close())

#include "../engine/db.h"
#include "../engine/variant.h"

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}


// At this point, count, r variables were set as globals
// Originally, they were local variables in the main() function
// Those variables are used by the thread functions so they were
// set as globals so they are recognised by the threads

long int count;
int r;

// Depending on the write percentage the user gives as input
// there is also a specific number of read/write operations
// that will be executed by the threads

long int write_count;
long int read_count;

// The variable that stores the write percentage the user gives as input
float write_percentage;

//The variable that stores the read percentage
int read_percentage;


// The variables that determine how many reads and writes
// the threads will execute
int writes_per_thread;
int reads_per_thread;

// The variables that store the total costs of executing the read/write operations
double total_write_cost;
double total_read_cost;

// The DB was originally used in the kiwi.c file
// It is now needed on the bench.c file for the purpose
// of Multithreading
DB* db;

// The variables that determine how many reader and writer
// threads will be used, depending on the write percentage
int reading_threads;
int writing_threads;

// The functions that are executed by the threads
// in order to run the _write_test(),_read_test()
// functions that are responsible for the read/write requests
void * thread_write_func(void * arg){
	_write_test(writes_per_thread,r,db);		
}

void * thread_read_func(void * arg){
	_read_test(reads_per_thread,r,db);
}

// Depending on which mode is executed (read,write,read-write)
// There are three functions to support it

// The following function performs the read-write operation
// Its return value is an int that indicates whether the
// multithreading is executed successfully or not
int execute_reads_writes(){

	// The thread_no is a variable that is used on the for loops bellow
	// to indicate the thread that is running
	int thread_no;

	// The thread_created_succefully is used to determine whether a
	// thread is successfully created
	int thread_created_succefully;
		
	// The following variables are two arrays of pthread_t type
	// They store the reader and writer threads
	// The appropriate memory is allocated using the malloc() call of the operating system
	pthread_t * writer_threads = (pthread_t *) malloc(writing_threads * sizeof(pthread_t));
	pthread_t * reader_threads = (pthread_t *) malloc(reading_threads * sizeof(pthread_t));


	// The writer and reader threads are created in the respective for loop
	// The following for loop initiates the writer threads

	for (thread_no=0;thread_no<writing_threads;thread_no++){

		thread_created_succefully = pthread_create(&writer_threads[thread_no], NULL, thread_write_func, NULL);

		// pthread_create() function returns 0 on successful creation
		// and a positive value otherwise
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation
		if (thread_created_succefully!=0){
			printf("HERE");
			return -1;
		}

			//usleep(2);
		printf("thread %d created",thread_no);
	}

	// The following for loop initiates the reader threads

	for (thread_no=0;thread_no< reading_threads;thread_no++){

		thread_created_succefully = pthread_create(&reader_threads[thread_no], NULL, thread_read_func, NULL);

		// pthread_create() function returns 0 on successful creation
		// and a positive value otherwise
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_created_succefully!=0){
			return -1;
		}

			//usleep(2);
		printf("thread %d created",thread_no);
	}

	// For each reader and writer thread, it is joined
	// so that it can wait the other threads to finish,
	// if it has already finished

	int i;

	// The thread_joined_succefully variable indicates
	// whether each thread in successfully joined 
	int thread_joined_succefully;
	for (i = 0; i < writing_threads ; i++)		
		thread_joined_succefully = pthread_join(writer_threads[i], NULL);

		// pthread_join() function ret<urns 0 on successful thread joining
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_joined_succefully!=0){
			return -1;
	}

	for (i = 0; i < reading_threads ; i++)		
		thread_joined_succefully = pthread_join(reader_threads[i], NULL);

		// pthread_join() function returns 0 on successful thread joining
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_joined_succefully!=0){
			return -1;
	}

	// Finally, the memory allocated for the arrays
	// that store the reader/writer threads in now free
	// using the free() function

	free(writer_threads);
	free(reader_threads);

	// Returning the value of 1 indicates that the multithreading succeded
	return 1;
}


// The following function performs the write operation
// Its return value is an int that indicates whether the
// multithreading is executed successfully or not
int execute_writes_only(){

	// The thread_no is a variable that is used on the for loop bellow
	// to indicate the thread that is running
	int thread_no;

	// The thread_created_succefully is used to determine whether a
	// thread is successfully created
	int thread_created_succefully;
	
	// The following variable is an array of pthread_t type
	// It stores the writer threads
	// The appropriate memory is allocated using the malloc() call of the operating system
	pthread_t * writer_threads = (pthread_t *) malloc(writing_threads * sizeof(pthread_t));

	for (thread_no=0;thread_no<writing_threads;thread_no++){

		thread_created_succefully = pthread_create(&writer_threads[thread_no], NULL, thread_write_func, NULL);

		// pthread_create() function returns 0 on successful creation
		// and a positive value otherwise
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_created_succefully!=0){
			return -1;
		}
			//usleep(2);
		printf("thread %d created",thread_no);
	}

	int i;

	// The thread_joined_succefully variable indicates
	// whether each thread in successfully joined
	int thread_joined_succefully;
	for (i = 0; i < writing_threads; i++)		
		thread_joined_succefully = pthread_join(writer_threads[i], NULL);

		// pthread_join() function returns 0 on successful thread joining
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_joined_succefully!=0){
			return -1;
	}

	// Finally, the memory allocated for the array
	// that store the writer threads in now free
	// using the free() function

	free(writer_threads);

	// Returning the value of 1 indicates that the multithreading succeded
	return 1;
}

// The following function performs the read operation
// Its return value is an int that indicates whether the
// multithreading is executed successfully or not
int execute_reads_only(){

	// The thread_no is a variable that is used on the for loop bellow
	// to indicate the thread that is running
	int thread_no;

	// The thread_created_succefully is used to determine whether a
	// thread is successfully create
	int thread_created_succefully;
		
	// The following variable is an array of pthread_t type
	// It stores the reader threads
	// The appropriate memory is allocated using the malloc() call of the operating system
	pthread_t * reader_threads = (pthread_t *) malloc(reading_threads * sizeof(pthread_t));

	for (thread_no=0;thread_no<reading_threads;thread_no++){

		thread_created_succefully = pthread_create(&reader_threads[thread_no], NULL, thread_read_func, NULL);

		// pthread_create() function returns 0 on successful creation
		// and a positive value otherwise
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_created_succefully!=0){
			return -1;
		}

			//usleep(2);
		printf("thread %d created",thread_no);
	}

	int i;

	// The thread_joined_succefully variable indicates
	// whether each thread in successfully joined
	int thread_joined_succefully;

	for (i = 0; i < reading_threads; i++)		
		thread_joined_succefully = pthread_join(reader_threads[i], NULL);

		// pthread_join() function returns 0 on successful thread joining
		// so it is checked if the value is not 0 to determine unsuccessful creation
		// and -1 is returned as an indicator of unsuccessful thread creation

		if (thread_joined_succefully!=0){
			return -1;
	}

	// Finally, the memory allocated for the array
	// that store the reader threads in now free
	// using the free() function

	free(reader_threads);

	// Returning the value of 1 indicates that the multithreading succeded
	return 1;
}

// The initialise_threads() function is responsible for
// executing the multithreading read-only, write-only
// or simultaneous read-write operation
// under the rules of the mutual exclusion

// Its argument is the write percentage that the user give as an input
// Its return value is the return value of the called functions implemented above
// which indicates the successful execution of multithreading or not
int initialise_threads(int write_percentage){

	int ok_execution;

	// The number of write operations is directly determined
	// by the write percentage 
	// the round() function is used in order to have a better
	// aproximation of the reads and writes that will be executed
	write_count = round(count * write_percentage/100);

	// the read operations are the rest of the operations
	// meaning that once the write operations are calculated
	// the write operations are extracted from the total
	// number of operation to get the read operations number

	read_count = count - write_count;

	printf(" INITIALIZE %d %d",write_count,read_count);

	// If the write percenatage is an integer in between 0 and 100
	// that means the read-write operation is executed

	if (write_percentage > 0 && write_percentage < 100){

		// After the number of read and write requests is calculated
		// each thread gets an equal ammount of read and write
		// requests by deviding the read/write requests by the
		// number of threads

		writing_threads = round(NUMBER_OF_THREADS * write_percentage/100);
		reading_threads = NUMBER_OF_THREADS - writing_threads;

		reads_per_thread = read_count/reading_threads;

		writes_per_thread = write_count/writing_threads;

		ok_execution = execute_reads_writes();

		if (ok_execution > 0){

			printf("\n\ntotal_read_cost: %f\n\n",total_read_cost);
			printf("total_write_cost: %f\n\n",total_write_cost);
			printf("total cost: %f\n\n",total_write_cost + total_read_cost);
			printf("|Random-Read: %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
	                (double)(total_read_cost / read_count),
	                (double)(read_count/ total_read_cost),
	                total_read_cost);

	        	printf("|Random-Write: %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n", 
	                (double)(total_write_cost/ write_count),
	                (double)(write_count / total_write_cost),
	                total_write_cost);
	       		printf("\n\n");
	    }
	}

	// If the write percenatage is equal to 100
	// that means there are no read operations and the write-only
	// operation is executed

	else if (write_percentage == 100){
		write_count = count;
		writing_threads = NUMBER_OF_THREADS;
		writes_per_thread = write_count/writing_threads;
		ok_execution = execute_writes_only();

		if (ok_execution > 0){
			printf("\n\ntotal_write_cost: %f\n\n",total_write_cost);
			printf("|Random-Write: %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n", 
	                (double)(total_write_cost/ write_count),
	                (double)(write_count / total_write_cost),
	                total_write_cost);
		}
		//printf("total_write_cost %f\n\n",total_write_cost);
	}

	// If the write percenatage is equal to 0
	// that means there are no write operations and the read-only
	// operation is executed

	else if (write_percentage == 0){
		read_count = count;
		reading_threads = NUMBER_OF_THREADS;
		reads_per_thread = read_count/reading_threads;
		ok_execution = execute_reads_only();

		if (ok_execution > 0){
			printf("\n\ntotal_read_cost: %f\n\n",total_read_cost);
			printf("|Random-Read: %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
	                (double)(total_read_cost / read_count),
	                (double)(read_count/ total_read_cost),
	                total_read_cost);
		}
		//printf("total_read_cost %f\n\n",total_read_cost);
	}

	// Returning the value of 1 indicates that the multithreading succeded
	return 1;
}


// In the main() function, the count,r variables were set as globals,
// as mentioned above, for the purpose of the multithreading
// Original, they had local scope in the main() function

int main(int argc,char** argv)
{
	srand(time(NULL));

	// The main() function gets 4 input arguments now instead of 3
	// so the check is also changed properly
	if (argc < 4) {
		fprintf(stderr,"Usage: db-bench <readwrite> <count> <write percentage> <r> (optional)\n");
		exit(1);
	}

	// The value of r variable is set to 1 when 5 input arguments are given
	// instead of 4 previously

	// In each case, the database has to open and close once
	// As opening and closing it in each thread can cause
	// memory menagement
	

	if (strcmp(argv[1], "readwrite") == 0) {
		r = 0;

		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();

		if (argc == 5)
			r = 1;

		db = db_open(DATAS);

		int threads = initialise_threads(atoi(argv[3]));

		if (threads >0){
			printf("\n\nMultithreading finished successfully");
		}

		db_close(db);
		//_read_test(count, r);
	}

	 else {
		fprintf(stderr,"Usage: <readwrite> <count> <write percentage> <r> (optional)n");
		exit(1);
	}

	return 1;
}


