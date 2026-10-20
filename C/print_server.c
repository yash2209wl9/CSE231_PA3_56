#include "job_queue.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NUM_PRINTERS 4
#define DEFAULT_JOBS 50

typedef struct {
    int id;
    job_queue_t *queue;
    long jobs_completed;
} worker_arg_t;

typedef struct {
    int p_id;
    long cnt;
    job_queue_t *q;
    int err;
} prod_info_t; //struct for bonus producer args

static void simulate_printing(void)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L}; /* 1 ms */
    (void)nanosleep(&ts, NULL);
}

static void *printer_worker(void *arg)
{
    worker_arg_t *w = (worker_arg_t *)arg;
    int job = 0;

// queue_pop returns 1 while there are jobs and 0 only when shutdown is 1 and count is 0 so all queued jobs get drained before thread exits
    while (queue_pop(w->queue, &job)) {
        simulate_printing();
        w->jobs_completed++; //private counter per worker avoids mutex contention on every job
        printf("PROCESSED job=%d printer=%d\n", job, w->id);
        fflush(stdout);
    }

    return NULL;
}

static void *prod_fn(void *arg)
{
    prod_info_t *info = (prod_info_t *)arg;

// calc unique job id using p_id * cnt + i + 1 so ids go from 1 to P*K w/o needing a separate mutex just for job numbers
    for (long i = 0; i < info->cnt; i++) {
        int j_id = (int)(info->p_id* info->cnt + i + 1); //1-indexed positive id
        if (queue_push(info->q, j_id) != 0) {
            fprintf(stderr, "Failed to submit job %d\n", j_id);
            info->err =1;
            return NULL;
        }
        // exact string for autograder do not change
        printf("SUBMITTED producer=%d job=%d\n", info->p_id, j_id);
        fflush(stdout);
    }
    return NULL;
}

static long parse_positive_long(const char *s, const char *name)
{
    char *end = NULL;
    errno = 0;
    long value = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || value <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, s);
        exit(EXIT_FAILURE);
    }
    return value;
}

static int run_core_server(long num_jobs)
{
    job_queue_t queue;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};

    if (queue_init(&queue) != 0) {
        fprintf(stderr, "queue_init failed\n");
        return EXIT_FAILURE;
    }

    /* TODO C13:
     * Initialize workers[i] and create NUM_PRINTERS printer_worker threads.
     */
// pass &workers[i] instead of &i otherwise loop changes i before thread reads it like in lec 14 slides
    for (int i = 0; i < NUM_PRINTERS; i++) {
        workers[i].id = i;
        workers[i].queue = &queue; //all 4 printers share same queue address
        workers[i].jobs_completed =0;
        if (pthread_create(&printers[i], NULL, printer_worker, &workers[i]) != 0) {
            fprintf(stderr, "pthread_create failed\n");
            queue_shutdown(&queue);
            for (int j = 0; j < i; j++) {
                pthread_join(printers[j], NULL); //reap already started threads on error
            }
            queue_destroy(&queue);
            return EXIT_FAILURE;
        }
    }

    for (long job = 1; job <= num_jobs; job++) {
        if (queue_push(&queue, (int)job) != 0) {
            fprintf(stderr, "Failed to submit job %ld\n", job);
            queue_shutdown(&queue);
            for (int i = 0; i < NUM_PRINTERS; i++) {
                pthread_join(printers[i], NULL);
            }
            queue_destroy(&queue);
            return EXIT_FAILURE;
        }
        printf("SUBMITTED job=%ld\n", job);
        fflush(stdout); //flush so output doesnt get mixed up
    }

    /* TODO C14: request graceful shutdown after the last submission. */
    queue_shutdown(&queue); //only after all N pushed

    /* TODO C15: join all printer threads. */
// if main returns w/o join the whole process dies and kills worker threads mid print
    int join_err = 0;
    for (int i = 0; i < NUM_PRINTERS; i++) {
        if (pthread_join(printers[i], NULL) != 0) {
            fprintf(stderr, "pthread_join failed\n");
            join_err = 1;
        }
    }
    if (join_err) {
        queue_destroy(&queue);
        return EXIT_FAILURE;
    }

    long total= 0;
    for (int i = 0; i < NUM_PRINTERS; i++) {
        total += workers[i].jobs_completed; //sum local counts after join
    }
    // sanity check total should equal num_jobs (50 by default)

    printf("TOTAL_COMPLETED %ld\n", total);
    fflush(stdout);

    queue_destroy(&queue);
    return (total == num_jobs) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/*
 * BONUS (+10): Multiple concurrent client/producers.
 *
 * Required command line when implemented:
 *     ./print_server --bonus <producers> <jobs-per-producer>
 *
 * Each producer must run as its own pthread and submit exactly the requested
 * number of jobs into the SAME bounded queue while the printer threads consume
 * concurrently. Use unique positive job IDs. For every accepted job print:
 *
 *     SUBMITTED producer=<p> job=<id>
 *
 * The printer output remains:
 *
 *     PROCESSED job=<id> printer=<id>
 *
 * Main must join all producer threads BEFORE calling queue_shutdown(). Then it
 * must join the printer threads and print:
 *
 *     TOTAL_COMPLETED <n>
 *
 * The autograder awards bonus credit only when every submitted job is processed
 * exactly once and all producer counts are correct.
 */
static int run_bonus_server(long producers, long jobs_per_producer)
{
    job_queue_t q;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};

    if (queue_init(&q) != 0) {
        fprintf(stderr, "queue_init failed\n");
        return EXIT_FAILURE;
    }

    pthread_t *p_tids = malloc(sizeof(pthread_t) * (size_t)producers); //heap alloc since P is runtime arg
    prod_info_t *p_args = malloc(sizeof(prod_info_t) * (size_t)producers);
    if (p_tids == NULL || p_args == NULL) {
        free(p_tids);
        free(p_args);
        queue_destroy(&q);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < NUM_PRINTERS; i++) {
        workers[i].id = i;
        workers[i].queue= &q;
        workers[i].jobs_completed = 0;
        if (pthread_create(&printers[i], NULL, printer_worker, &workers[i]) != 0) {
            fprintf(stderr, "pthread_create printer failed\n");
            queue_shutdown(&q);
            for (int j = 0; j < i; j++) {
                pthread_join(printers[j], NULL);
            }
            free(p_tids);
            free(p_args);
            queue_destroy(&q);
            return EXIT_FAILURE;
        }
    }

// no global producer lock needed here since queue_push already locks q->mutex internally
    for (long idx = 0; idx < producers; idx++) {
        p_args[idx].p_id = (int)idx; //producer ids 0..P-1
        p_args[idx].cnt =jobs_per_producer;
        p_args[idx].q = &q;
        p_args[idx].err= 0;
        if (pthread_create(&p_tids[idx], NULL, prod_fn, &p_args[idx]) != 0) {
            fprintf(stderr, "pthread_create producer failed\n");
            queue_shutdown(&q);
            for (long j = 0; j < idx; j++) {
                pthread_join(p_tids[j], NULL);
            }
            for (int i = 0; i < NUM_PRINTERS; i++) {
                pthread_join(printers[i], NULL);
            }
            free(p_tids);
            free(p_args);
            queue_destroy(&q);
            return EXIT_FAILURE;
        }
    }

    int bad = 0;
// have to join all producer threads before calling shutdown otherwise if one finishes early and shuts down the rest get rejected
    for (long idx = 0; idx < producers; idx++) {
        if (pthread_join(p_tids[idx], NULL) != 0) {
            fprintf(stderr, "pthread_join producer failed\n");
            bad = 1;
        }
        if (p_args[idx].err != 0) {
            bad = 1;
        }
    }
    // ordering: join all producers -> queue_shutdown -> join all consumers

    queue_shutdown(&q); //now safe to shutdown

    for (int i = 0; i < NUM_PRINTERS; i++) {
        if (pthread_join(printers[i], NULL) != 0) {
            fprintf(stderr, "pthread_join printer failed\n");
            bad = 1;
        }
    }

    long tot= 0;
    for (int i = 0; i < NUM_PRINTERS; i++) {
        tot = tot + workers[i].jobs_completed;
    }

    // expected is P * K (e.g. 3 * 25 = 75)
    printf("TOTAL_COMPLETED %ld\n", tot);
    fflush(stdout);

    free(p_tids);
    free(p_args);
    queue_destroy(&q);

    if (bad == 0 && tot == producers * jobs_per_producer) {
        return EXIT_SUCCESS;
    }
    return EXIT_FAILURE;
}

int main(int argc, char *argv[])
{
    if (argc >= 2 && strcmp(argv[1], "--bonus") == 0) {
        if (argc != 4) {
            fprintf(stderr, "Usage: %s --bonus <producers> <jobs-per-producer>\n", argv[0]);
            return EXIT_FAILURE;
        }
        long producers = parse_positive_long(argv[2], "producer count");
        long jobs = parse_positive_long(argv[3], "jobs per producer");
        return run_bonus_server(producers, jobs);
    }

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [number-of-jobs]\n", argv[0]);
        return EXIT_FAILURE;
    }

    long num_jobs = (argc == 2) ? parse_positive_long(argv[1], "job count") : DEFAULT_JOBS;
    return run_core_server(num_jobs);
}
