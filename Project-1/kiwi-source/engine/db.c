#include <string.h>
#include <assert.h>
#include "db.h"
#include "indexer.h"
#include "utils.h"
#include "log.h"
#include <pthread.h>

// In order for the readers-writers algorithm to execute
// the wait()/broadcast() system calls are required 
// These calls also require to be surrounded by a mutex
// So a common mutex is used because if a writer is inside,
// the readers will wait for the writer to finish and then enter in their critical section
// That also goes on for the writer, who enters once there are no readers and no other writer
pthread_mutex_t writers_mutex = PTHREAD_MUTEX_INITIALIZER;

// The condition variable for the readers, which is used to
// notify them when to wait and when to enter the critical section
// under the rules of the mutual exclusion
pthread_cond_t cond_var_readers = PTHREAD_COND_INITIALIZER;

// The condition variable for the writer, which is used to
// notify it when to wait and when to enter the critical section
// under the rules of the mutual exclusion
pthread_cond_t cond_var_writers = PTHREAD_COND_INITIALIZER;

// The number of readers currently inside the DB
// it is checked by the writers so they have to wait when readers
// are cunrently inside the DB and notify one of them to enter
// when no readers are inside the DB
int read_enabled =0;

// The number of writers currently inside the DB
// it is checked by the readers so they have to wait when a writer
// are cunrently inside the DB and notify them to enter
// when the writer is no longer inside the DB
// This variable has the values of 0 or 1, meaning either only one
// or no writer is inside the library
int write_enabled =0;

DB* db_open_ex(const char* basedir, uint64_t cache_size)
{
    DB* self = calloc(1, sizeof(DB));

    if (!self)
        PANIC("NULL allocation");

    strncpy(self->basedir, basedir, MAX_FILENAME);
    self->sst = sst_new(basedir, cache_size);

    Log* log = log_new(self->sst->basedir);
    self->memtable = memtable_new(log);

    return self;
}

DB* db_open(const char* basedir)
{
    return db_open_ex(basedir, LRU_CACHE_SIZE);
}

void db_close(DB *self)
{
    INFO("Closing database %d", self->memtable->add_count);

    if (self->memtable->list->count > 0)
    {
        sst_merge(self->sst, self->memtable);
        skiplist_release(self->memtable->list);
        self->memtable->list = NULL;
    }

    sst_free(self->sst);
    log_remove(self->memtable->log, self->memtable->lsn);
    log_free(self->memtable->log);
    memtable_free(self->memtable);
    free(self);
}

int db_add(DB* self, Variant* key, Variant* value)
{    
    // As explained above, wait()/brodcast() system calls
    // are surrounded by lock()/unlock() system calls
    pthread_mutex_lock(&writers_mutex);

    // When a writer attempts to enter the DB, it has to first check if
    // no readers are inside the DB. If there is at least one, the writer has to wait
    while(read_enabled >0){
        #if DEBUGGING_PRINTS_ENABLED == 1
            printf("IN WRITERS read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
        #endif
        pthread_cond_wait(&cond_var_writers,&writers_mutex);
    }

    // Once there are no readers inside the DB, the writers are notified and can
    // now enter the DB
    pthread_mutex_unlock(&writers_mutex);

    // Since only one writer is allowed at a time, the writer has to lock again,
    // before entering the critical section
    pthread_mutex_lock(&writers_mutex);

    // The writer increments its counter by 1 and since there is only one writer at a time
    // the value of write_enabled variable can be either 0 or 1
    write_enabled ++;

    #if DEBUGGING_PRINTS_ENABLED == 1
        printf(" WRITER STARTED read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
    #endif

    // The return value of memtable_add() function is now stored in 
    // the value_added variable
    // Originally, it was returned directly by the function
    // Returning it before a mutex is unlocked can cause problems
    // to the system, so it has to be returned after the mutex is unlocked
    int value_added;

    if (memtable_needs_compaction(self->memtable))
    {
        INFO("Starting compaction of the memtable after %d insertions and %d deletions",
             self->memtable->add_count, self->memtable->del_count);
        sst_merge(self->sst, self->memtable);

        memtable_reset(self->memtable);
    }

    //The return value of memtable_add() function stored in
    // the value_added variable once the writer finishes 
    value_added = memtable_add(self->memtable, key, value);

    // The writer has finished so it decrease the value of
    // write_enabled variable to 0
    write_enabled --;

    #if DEBUGGING_PRINTS_ENABLED == 1
        printf(" WRITER FINISHED read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
    #endif

    // There is no writer inside so it notifies the readers that they can now enter
    // and unlocks the mutex that goes along with the condition variable of the readers
    pthread_cond_broadcast(&cond_var_readers);
    pthread_mutex_unlock(&writers_mutex);

    // The return value of memtable_add() function can now be returned
    // since there is no locked mutex and causes no problem to the system
    return value_added;
}

int db_get(DB* self, Variant* key, Variant* value)
{   
    // As explained above, wait()/brodcast() system calls
    // are surrounded by lock()/unlock() system calls
    pthread_mutex_lock(&writers_mutex);

    // When a reader is attempting to enter the DB,
    // first, it increments the read_enabled variable
    // as there can be multiple readers inside the DB
    read_enabled ++;

    // Each reader checks if a writer is inside the DB and if so,
    // they have to wait the writer to finish and then enter the DB
    while(write_enabled == 1){
        #if DEBUGGING_PRINTS_ENABLED == 1
            printf("IN READERS read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
        #endif
        pthread_cond_wait(&cond_var_readers,&writers_mutex);
    }

    // Once the writer is no longer inside the DB, the readers may now proceed
    pthread_mutex_unlock(&writers_mutex);

    // Unlike the writer, the reader does not have to lock the reading section
    // as there can be multiple readers in the DB, as long there is no writer

    // A reader searches first in the memtable and theb in the sst
    // it the value is not found in the memtable
    // A repective value is returned using the return_value variable
    // For the same reason the value_added variable is used in db_add() function
    int return_value;

    // The found_in_memtable variable is used to determine if
    // the value was found in the memtable
    // Its use is more to make the code easier to read and be understood
    int found_in_memtable;

    #if DEBUGGING_PRINTS_ENABLED == 1
        printf("READERS STARTED read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
    #endif

    found_in_memtable = memtable_get(self->memtable->list, key, value);

    if (found_in_memtable == 1){
        return_value =1;
    }
    else{
        return_value = sst_get(self->sst, key, value);
    }

    pthread_mutex_lock(&writers_mutex);

    // One of the readers finished and the number of readers decreases

    read_enabled--;

    #if DEBUGGING_PRINTS_ENABLED == 1
        printf("READERS FINISHED read_enabled %d write_enabled %d\n\n",read_enabled,write_enabled);
    #endif

    // Once a reader finishes searching a value, it will notify the writers
    // The writers will then check their condition, meaning if there are no readers
    // inside the DB and enter once there is no reader
    // If there is at least one reader inside, they will wait
    
    pthread_cond_broadcast(&cond_var_writers);
    
    pthread_mutex_unlock(&writers_mutex);

    // The value of return_value variable can now be returned
    // since there is no locked mutex and causes no problem to the system
    return return_value;
}

int db_remove(DB* self, Variant* key)
{
    return memtable_remove(self->memtable, key);
}

DBIterator* db_iterator_new(DB* db)
{
    DBIterator* self = calloc(1, sizeof(DBIterator));
    self->iterators = vector_new();
    self->db = db;

    self->sl_key = buffer_new(1);
    self->sl_value = buffer_new(1);

    self->list = db->memtable->list;
    self->prev = self->node = self->list->hdr;

    skiplist_acquire(self->list);

    // Let's acquire the immutable list if any
    pthread_mutex_lock(&self->db->sst->immutable_lock);

    if (self->db->sst->immutable_list)
    {
        skiplist_acquire(self->db->sst->immutable_list);

        self->imm_list = self->db->sst->immutable_list;
        self->imm_prev = self->imm_node = self->imm_list->hdr;
        self->has_imm = 1;
    }

    pthread_mutex_unlock(&self->db->sst->immutable_lock);

    // TODO: At this point we should get the current sequence of the active
    // SkipList in order to avoid polluting the iteration

    self->use_memtable = 1;
    self->use_files = 1;

    self->advance = ADV_MEM | ADV_MEM;

    return self;
}

void db_iterator_free(DBIterator* self)
{
    for (int i = 0; i < vector_count(self->iterators); i++)
        chained_iterator_free((ChainedIterator *)vector_get(self->iterators, i));

    heap_free(self->minheap);
    vector_free(self->iterators);

    buffer_free(self->sl_key);
    buffer_free(self->sl_value);

    if (self->has_imm)
    {
        buffer_free(self->isl_key);
        buffer_free(self->isl_value);
    }

    skiplist_release(self->list);

    if (self->imm_list)
        skiplist_release(self->imm_list);

    free(self);
}

static void _db_iterator_add_level0(DBIterator* self, Variant* key)
{
    // Createa all iterators for scanning level0. If is it possible
    // try to create a chained iterator for non overlapping sequences.

    int i = 0;
    SST* sst = self->db->sst;

    while (i < sst->num_files[0])
    {
        INFO("Comparing %.*s %.*s", key->length, key->mem, sst->files[0][i]->smallest_key->length, sst->files[0][i]->smallest_key->mem);
        if (variant_cmp(key, sst->files[0][i]->smallest_key) < 0)
        {
            i++;
            continue;
        }
        break;
    }

    i -= 1;

    if (i < 0 || i >= sst->num_files[0])
        return;

    int j = i + 1;
    Vector* files = vector_new();
    vector_add(files, sst->files[0][i]);

    INFO("%s", sst->files[0][0]->loader->file->filename);

    while ((i < sst->num_files[0]) && (j < sst->num_files[0]))
    {
        if (!range_intersects(sst->files[0][i]->smallest_key,
                            sst->files[0][i]->largest_key,
                            sst->files[0][j]->smallest_key,
                            sst->files[0][j]->largest_key))
            vector_add(files, sst->files[0][j]);
        else
        {
            size_t num_files = vector_count(files);
            SSTMetadata** arr = vector_release(files);

            vector_add(self->iterators,
                       chained_iterator_new_seek(num_files, arr, key));

            i = j;
            vector_add(files, sst->files[0][i]);
        }

        j++;
    }

    if (vector_count(files) > 0)
    {
        vector_add(self->iterators,
                   chained_iterator_new_seek(vector_count(files),
                                             (SSTMetadata **)files->data, key));

        files->data = NULL;
    }

    vector_free(files);
}

void db_iterator_seek(DBIterator* self, Variant* key)
{
#ifdef BACKGROUND_MERGE
    pthread_mutex_lock(&self->db->sst->lock);
#endif

    _db_iterator_add_level0(self, key);

    int i = 0;
    SST* sst = self->db->sst;
    Vector* files = vector_new();

    for (int level = 1; level < MAX_LEVELS; level++)
    {
        i = sst_find_file(sst, level, key);

        if (i >= sst->num_files[level])
            continue;

        for (; i < sst->num_files[level]; i++)
        {
            DEBUG("Iterator will include: %d [%.*s, %.*s]",
                  sst->files[level][i]->filenum,
                  sst->files[level][i]->smallest_key->length,
                  sst->files[level][i]->smallest_key->mem,
                  sst->files[level][i]->largest_key->length,
                  sst->files[level][i]->largest_key->mem);
            vector_add(files, (void*)sst->files[level][i]);
        }

        size_t num_files = vector_count(files);
        SSTMetadata** arr = vector_release(files);

        vector_add(self->iterators,
                   chained_iterator_new_seek(num_files, arr, key));
    }

#ifdef BACKGROUND_MERGE
    pthread_mutex_unlock(&self->db->sst->lock);
#endif
    vector_free(files);

    self->minheap = heap_new(vector_count(self->iterators), (comparator)chained_iterator_comp);

    for (i = 0; i < vector_count(self->iterators); i++)
        heap_insert(self->minheap, (ChainedIterator*)vector_get(self->iterators, i));

    self->node = skiplist_lookup_prev(self->db->memtable->list, key->mem, key->length);

    if (!self->node)
        self->node = self->db->memtable->list->hdr;

    self->prev = self->node;

    db_iterator_next(self);
}

static void _db_iterator_next(DBIterator* self)
{
    ChainedIterator* iter;

start:

    if (self->current != NULL)
    {
        iter = self->current;
        sst_loader_iterator_next(iter->current);

        if (iter->current->valid)
        {
            iter->skip = 0;
            heap_insert(self->minheap, iter);
        }
        else
        {
            // Let's see if we can go on with the chained iterator
            if (iter->pos < iter->num_files)
            {
                // TODO: Maybe a reinitialization would be better
                sst_loader_iterator_free(iter->current);
                iter->current = sst_loader_iterator((*(iter->files + iter->pos++))->loader);

                assert(iter->current->valid);
                heap_insert(self->minheap, iter);
            }
            else
                sst_loader_iterator_free(iter->current);
        }
    }

    if (heap_pop(self->minheap, (void**)&iter))
    {
        assert(iter->current->valid);

        self->current = iter;
        self->valid = 1;

        if (iter->skip == 1)
            goto start;

        if (iter->current->opt == DEL)
            goto start;
    }
    else
        self->valid = 0;
}

static void _db_iterator_advance_mem(DBIterator* self)
{
    while (1)
    {
        self->prev = self->node;
        self->list_end = self->node == self->list->hdr;

        if (self->list_end)
            return;

        OPT opt;
        memtable_extract_node(self->node, self->sl_key, self->sl_value, &opt);
        self->node = self->node->forward[0];

        if (opt == ADD)
            break;

        buffer_clear(self->sl_key);
        buffer_clear(self->sl_value);
    }
}

static void _db_iterator_advance_imm(DBIterator* self)
{
    while (self->has_imm)
    {
        self->imm_prev = self->imm_node;
        self->imm_list_end = self->imm_node == self->imm_list->hdr;

        if (self->imm_list_end)
            return;

        OPT opt;
        memtable_extract_node(self->imm_node, self->isl_key, self->isl_value, &opt);
        self->imm_node = self->imm_node->forward[0];

        if (opt == ADD)
            break;

        buffer_clear(self->isl_key);
        buffer_clear(self->isl_value);
    }
}

static void _db_iterator_next_mem(DBIterator* self)
{
    if (self->advance & ADV_MEM) _db_iterator_advance_mem(self);
    if (self->advance & ADV_IMM) _db_iterator_advance_imm(self);

    // Here we need to compare the two keys
    if (self->sl_key && !self->isl_key)
    {
        self->advance = ADV_MEM;
        self->key = self->sl_key;
        self->value = self->sl_value;
    }
    else if (!self->sl_key && self->isl_key)
    {
        self->advance = ADV_IMM;
        self->key = self->isl_key;
        self->value = self->isl_value;
    }
    else
    {
        if (variant_cmp(self->sl_key, self->isl_key) <= 0)
        {
            self->advance = ADV_MEM;
            self->key = self->sl_key;
            self->value = self->sl_value;
        }
        else
        {
            self->advance = ADV_IMM;
            self->key = self->isl_key;
            self->value = self->isl_value;
        }
    }
}

void db_iterator_next(DBIterator* self)
{
    if (self->use_files)
        _db_iterator_next(self);
    if (self->use_memtable)
        _db_iterator_next_mem(self);

    int ret = (self->list_end) ? 1 : -1;

    while (self->valid && !self->list_end)
    {
        ret = variant_cmp(self->key, self->current->current->key);
        //INFO("COMPARING: %.*s %.*s", self->key->length, self->key->mem, self->current->current->key->length,self->current->current->key->mem );

        // Advance the iterator from disk until it's greater than the memtable key
        if (ret == 0)
            _db_iterator_next(self);
        else
            break;
    }

    if (ret <= 0)
    {
        self->use_memtable = 1;
        self->use_files = 0;
    }
    else
    {
        self->use_memtable = 0;
        self->use_files = 1;
    }
}

int db_iterator_valid(DBIterator* self)
{
    return (self->valid || !self->list_end || (self->has_imm && !self->imm_list_end));
}

Variant* db_iterator_key(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->key;
    return self->key;
}

Variant* db_iterator_value(DBIterator* self)
{
    if (self->use_files)
        return self->current->current->value;
    return self->value;
}
