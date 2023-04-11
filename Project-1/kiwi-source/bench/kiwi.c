#include <string.h>
#include "bench.h"
#include <pthread.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include <errno.h>

//#define DATAS ("testdb")

// In the kiwi.c file, the total time it
// takes for the read/write requests to be executed
// is calculated here

// As stated in the comments of bench.h header file
// the values of total_write_cost and total_read_cost variables
// are calculated here and since they are defined in the bench.h header file
// bench.c can also use them since it includes the bench.h header file
double total_write_cost=0;
double total_read_cost=0;

// Since reading/writing is a multithreaded proccess
// the value of the cost it takes for a request to be completed
// is a shared resource
// Meaning the cost has to be protected as it is calculated,
// so that is not possible for another thread to alter it at the same time

// The cost of a reading request is protected by the readers_cost_mutex
pthread_mutex_t readers_cost_mutex = PTHREAD_MUTEX_INITIALIZER;

// The cost of a writing request is protected by the writers_cost_mutex
pthread_mutex_t writers_cost_mutex = PTHREAD_MUTEX_INITIALIZER;

// _write_test() function is modified to have the DB as an argument
// Originally, the DB opened and closed when the function was called
// But now opening and closing it multiple times can cause problems
// So it opens/closes on kiwi.c file and passed as an argument here
void _write_test(long int count, int r, DB* db)
{
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		if (r)
			_random_key(key, KSIZE);
		else{
			snprintf(key, KSIZE, "key-%d", i);
		}
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	end = get_ustime_sec();
	cost = end -start;

	// After the write cost is calculated, it is added 
	// on total_write_cost variable inside a critical section
	// to prevent its value being modified by another thread and
	// resulting in inaccurate total writing time
	pthread_mutex_lock(&writers_cost_mutex);

	total_write_cost += (double)(cost / count);

	pthread_mutex_unlock(&writers_cost_mutex);

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}


// _read_test() function is modified to have the DB as an argument
// Originally, the DB opened and closed when the function was called
// But now opening and closing it multiple times can cause problems
// So it opens/closes on kiwi.c file and passed as an argument here
void _read_test(long int count, int r, DB* db)
{
	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	//DB* db;
	char key[KSIZE + 1];

	//db = db_open(DATAS);
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	end = get_ustime_sec();
	cost = end - start;

	// After the read cost is calculated, it is added 
	// on total_read_cost variable inside a critical section
	// to prevent its value being modified by another thread and
	// resulting in inaccurate total reading time

	pthread_mutex_lock(&readers_cost_mutex);

	total_read_cost += (double)(cost / count);

	pthread_mutex_unlock(&readers_cost_mutex);

	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}
