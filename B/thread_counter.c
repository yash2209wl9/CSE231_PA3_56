#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_THREADS 4
#define DEFAULT_ITERATIONS 100000L
#define MAX_THREADS 64

static long total_jobs = 0;
static pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    long iterations;
    int use_mutex;
} worker_arg_t;

static void *counter_worker(void *arg)
{
    worker_arg_t *w = (worker_arg_t *)arg;

    for (long i = 0; i < w->iterations; i++) {
        if (w->use_mutex) {
            if (pthread_mutex_lock(&counter_mutex) != 0) {
                fprintf(stderr, "pthread_mutex_lock failed\n");
                exit(EXIT_FAILURE);
            }
        }

        total_jobs++; //unsafe mode: intentionally unsynchronized 

        if (w->use_mutex) {
            if (pthread_mutex_unlock(&counter_mutex) != 0) {
                fprintf(stderr, "pthread_mutex_unlock failed\n");
                exit(EXIT_FAILURE);
            }
        }
    }

    return NULL;
}

static long parse_long(const char *s, const char *name)
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

int main(int argc, char *argv[])
{
    const char *mode = (argc >= 2) ? argv[1] : "unsafe";
    long nthreads = (argc >= 3) ? parse_long(argv[2], "thread count") : DEFAULT_THREADS;
    long iterations = (argc >= 4) ? parse_long(argv[3], "iteration count") : DEFAULT_ITERATIONS;

    if (strcmp(mode, "unsafe") != 0 && strcmp(mode, "safe") != 0) {
        fprintf(stderr, "Usage: %s [unsafe|safe] [threads] [iterations]\n", argv[0]);
        return EXIT_FAILURE;
    }
    if (nthreads > MAX_THREADS) {
        fprintf(stderr, "Thread count must be <= %d\n", MAX_THREADS);
        return EXIT_FAILURE;
    }

    int use_mutex = (strcmp(mode, "safe") == 0);
    pthread_t threads[MAX_THREADS];
    worker_arg_t args[MAX_THREADS];

    total_jobs = 0;

    // args[i] lives in main's frame until after the joins, so pointers stay valid 
    for (long i = 0; i < nthreads; i++) {
        args[i].iterations = iterations;
        args[i].use_mutex = use_mutex;
        int rc = pthread_create(&threads[i], NULL, counter_worker, &args[i]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(rc));
            exit(EXIT_FAILURE);
        }
    }

    for (long i = 0; i < nthreads; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            fprintf(stderr, "pthread_join failed: %s\n", strerror(rc));
            exit(EXIT_FAILURE);
        }
    }

    long expected = nthreads * iterations;
    printf("Mode: %s\n", mode);
    printf("Expected jobs: %ld\n", expected);
    printf("Actual jobs:   %ld\n", total_jobs);

    if (use_mutex && total_jobs != expected) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
