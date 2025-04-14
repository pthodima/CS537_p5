#include "minispark.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

/*
 * Function for the list
*/

List* list_init(int n) {
  List* list = (List*)malloc(sizeof(List));
  if (list == NULL) {
    fprintf(stderr, "Failed to allocate memory for list.\n");
    exit(EXIT_FAILURE);
  }
  list->data = (void**)malloc(sizeof(void*)*n);
  if (!list->data) {
    fprintf(stderr, "Failed to allocate memory for list data.\n");
    free(list);
    exit(EXIT_FAILURE);
  }
  list->size = 0;
  list->capacity = n;
  return list;
}

void list_add_elem(List* list, void* elem) {
  if (list->size == list->capacity) {
    list->capacity *= 2;
    void** new_data = (void**)realloc(list->data, list->capacity * sizeof(void*));
    if (!new_data) {
      fprintf(stderr, "Failed to allocate memory for list data.\n");
      exit(EXIT_FAILURE);
    }
    list->data = new_data;
  }
  list->data[list->size] = elem;
  list->size++;
}

void list_free(List* list) {
  if (list) {
    free(list->data);
    free(list);
  }
}

void* list_get(List* list, int index) {
  if (index < 0 || index >= list->size) {
    return NULL;
  }
   return list->data[index];
}

/*
 * TaskQueue
 */

TaskQueue* taskqueue_init() {
  TaskQueue* q = (TaskQueue*)malloc(sizeof(TaskQueue));
  q->head = q->tail = NULL;
  pthread_mutex_init(&(q->mutex), NULL);
  pthread_cond_init(&(q->not_empty), NULL);
  q->shutdown = false;
  return q;
}

void taskqueue_enqueue(TaskQueue* q, Task* task) {
  TaskNode* node = (TaskNode*)malloc(sizeof(TaskNode));
  node->task = task;
  node->next = NULL;

  pthread_mutex_lock(&(q->mutex));
  if (q->tail) {
    q->tail->next = node;
    q->tail = node;
  } else {
    q->head = q->tail = node;
  }
  pthread_cond_signal(&(q->not_empty));
  pthread_mutex_unlock(&(q->mutex));
}

Task* taskqueue_dequeue(TaskQueue* q) {
  pthread_mutex_lock(&(q->mutex));
  while (!(q->head) && !(q->shutdown)) {
    pthread_cond_wait(&(q->not_empty), &(q->mutex));
  }

  if (q->shutdown && !q->head) {
    pthread_mutex_unlock(&(q->mutex));
    return NULL;
  }

  TaskNode* node = q->head;
  q->head = node->next;
  if (!q->head) q->tail = NULL;

  Task* task = node->task;
  free(node);
  pthread_mutex_unlock(&(q->mutex));
  return task;
}

void taskqueue_destroy(TaskQueue* q) {
  pthread_mutex_lock(&(q->mutex));
  q->shutdown = true;
  pthread_cond_signal(&(q->not_empty));
  pthread_mutex_unlock(&(q->mutex));

  TaskNode* node = q->head;
  while (node) {
    TaskNode* temp = node;
    node = node->next;
    free(temp->task);
    free(temp);
  }

  pthread_mutex_destroy(&(q->mutex));
  pthread_cond_destroy(&(q->not_empty));
  free(q);
}


// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile. 
void print_formatted_metric(TaskMetric* metric, FILE* fp) {
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
	  metric->rdd, metric->pnum, metric->rdd->trans,
	  metric->created.tv_sec, metric->created.tv_nsec / 1000,
	  metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
	  metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    maxpartitions = max(maxpartitions, dep->partitions->size);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions);
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);

  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(1);
    }
    list_add_elem(rdd->partitions, fp);
  }

  rdd->filebacked = true;
  rdd->numpartitions = numfiles;
  rdd->numdependencies = 0;
  rdd->trans = MAP;
  rdd->fn = (void *)identity;
  return rdd;
}

void execute(RDD* rdd) {
  if (rdd->is_materialized) return;

  if (rdd->filebacked) {
    rdd->is_materialized = true;
    return;
  }

  for (int i = 0; i < rdd->numdependencies; i++) {
    execute(rdd->dependencies[i]);
  }

  if (!rdd->partitions) {
    rdd->partitions = list_init(4);
    // for (int i = 0; i < rdd->numpartitions; i++) {
    //   list_add_elem(rdd->partitions, NULL);
    // }
  }

  switch (rdd->trans) {
    case MAP: {
     for(int p = 0; p < rdd->dependencies[0]->partitions->size; p++) {
        List* dep = list_get(rdd->dependencies[0]->partitions, p);
        void* e;
        for (int d = 0; d < dep->size; d++) {
          e =  dep->data[d];
          void* r = ((Mapper)rdd->fn)(e);
          if (r) list_add_elem(rdd->partitions, r);
        }
     }
     rdd->is_materialized = true;
    }
      break;
    default:
      break;
  }
  // rdd->is_materialized = true;
}

void MS_Run() {
  return;
}

void MS_TearDown() {
  return;
}

// TODO: implement count
int count(RDD *rdd) {
  execute(rdd);

  int count = 0;
  // count all the items in rdd
  return rdd->numpartitions;
}

void print(RDD *rdd, Printer p) {
  execute(rdd);

  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
  for (int i = 0; i < rdd->numpartitions; i++) {
    List* partition = list_get(rdd->partitions, i);
    if (partition) {
      void* elem;
      for (int i = 0; i < partition->size; i++) {
        elem = partition->data[i];
        p(elem);
      }
    }
  }
}
