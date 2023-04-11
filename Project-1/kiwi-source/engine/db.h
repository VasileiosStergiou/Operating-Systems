#ifndef __DB_H__
#define __DB_H__

#include "indexer.h"
#include "sst.h"
#include "variant.h"
#include "memtable.h"
#include "merger.h"


// the following macro allows the user to
// read the debugging messages or store them into
// a file by using the " > " to prinnt them into a file
// instead of the standard ouput
// the user can enable the dubugging prints in the db.c file
// by setting the macro to the value of 1 and on the value of 0
// to disable them
#define DEBUGGING_PRINTS_ENABLED 0

// In order for the readers-writers algorithm to execute
// the wait()/broadcast() system calls are required 
// These calls also require to be surrounded by a mutex
// So a common mutex is used because if a writer is inside,
// the readers will wait for the writer to finish and then enter in their critical section
// That also goes on for the writer, who enters once there are no readers and no other writer
extern pthread_mutex_t writers_mutex;

// The condition variable for the readers, which is used to
// notify them when to wait and when to enter the critical section
// under the rules of the mutual exclusion
extern pthread_cond_t cond_var_readers;

// The condition variable for the writer, which is used to
// notify it when to wait and when to enter the critical section
// under the rules of the mutual exclusion
extern pthread_cond_t cond_var_writers;

// The number of readers currently inside the DB
// it is checked by the writers so they have to wait when readers
// are cunrently inside the DB and notify one of them to enter
// when no readers are inside the DB
extern int read_enabled;

// The number of writers currently inside the DB
// it is checked by the readers so they have to wait when a writer
// are cunrently inside the DB and notify them to enter
// when the writer is no longer inside the DB
// This variable has the values of 0 or 1, meaning either only one
// or no writer is inside the library

extern int write_enabled;



typedef struct _db {
//    char basedir[MAX_FILENAME];
    char basedir[MAX_FILENAME+1];
    SST* sst;
    MemTable* memtable;
} DB;

DB* db_open(const char *basedir);
DB* db_open_ex(const char *basedir, uint64_t cache_size);

void db_close(DB* self);
int db_add(DB* self, Variant* key, Variant* value);
int db_get(DB* self, Variant* key, Variant* value);
int db_remove(DB* self, Variant* key);

typedef struct _db_iterator {
    DB* db;
    unsigned valid:1;

    unsigned use_memtable:1;
    unsigned use_files:1;
    unsigned has_imm:1;

    Heap* minheap;
    Vector* iterators;

    SkipNode* node;
    SkipNode* imm_node;
    SkipNode* prev;
    SkipNode* imm_prev;

    SkipList* list;
    SkipList* imm_list;

    unsigned list_end:1;
    unsigned imm_list_end:1;
    unsigned advance;

#define ADV_MEM 1
#define ADV_IMM 2

    Variant* sl_key;
    Variant* sl_value;

    Variant* isl_key;
    Variant* isl_value;

    Variant* key;
    Variant* value;

    ChainedIterator* current;
} DBIterator;

DBIterator* db_iterator_new(DB* self);
void db_iterator_free(DBIterator* self);

void db_iterator_seek(DBIterator* self, Variant* key);
void db_iterator_next(DBIterator* self);
int db_iterator_valid(DBIterator* self);

Variant* db_iterator_key(DBIterator* self);
Variant* db_iterator_value(DBIterator* self);

#endif
