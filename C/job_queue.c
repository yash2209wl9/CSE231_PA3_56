#include "job_queue.h"

#include <stddef.h>

int queue_init(job_queue_t *q)
{
    if (q == NULL) {
        return -1;
    }

    q->head = 0;
    q->tail =0;
    q->count= 0;
    q->shutdown = 0;

    /* TODO C3:
     * Initialize q->mutex, q->not_empty, and q->not_full.
     * If an initialization fails, clean up anything already initialized
     * and return -1. Return 0 on success.
     */
    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        return -1;
    }

    if (pthread_cond_init(&q->not_empty, NULL) !=0) {
        pthread_mutex_destroy(&q->mutex); //cleanup mutex if cond fails
        return -1;
    }

    if (pthread_cond_init(&q->not_full,NULL) != 0) {
        pthread_cond_destroy(&q->not_empty); //have to destroy both if 2nd cond fails
        pthread_mutex_destroy(&q->mutex);
        return -1;
    }
    // invariant is 0 <= count <= QUEUE_CAPACITY

    return 0;
}

void queue_destroy(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    /* TODO C4: destroy the mutex and both condition variables. */
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex); //only call after all threads joined
}

int queue_push(job_queue_t *q, int job)
{
    if (q == NULL) {
        return -1;
    }

    /* TODO C5-C7:
     * 1. Lock q->mutex.
     * 2. While the queue is full and shutdown is false, wait on not_full.
     * 3. If shutdown is set, unlock and return -1.
     * 4. Insert job at tail, advance tail circularly, increment count.
     * 5. Signal a consumer through not_empty.
     * 6. Unlock and return 0.
     *
     * IMPORTANT: pthread_cond_wait() must be guarded by a while loop.
     */
    pthread_mutex_lock(&q->mutex);

// need while loop here instead of if because of mesa semantics where another thread might grab the slot before we wake up and get the lock back
    while (q->count == QUEUE_CAPACITY && q->shutdown ==0) {
        pthread_cond_wait(&q->not_full, &q->mutex); //atomically unlocks mutex and sleeps
    }
    // recheck condition after wakeup in case of spurious wakeup

    if (q->shutdown != 0) { //dont push if shutting down
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->jobs[q->tail]= job;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY; //wrap around MAX=8
    q->count = q->count+ 1;
    // fill = (fill + 1) % MAX from lec 13 slides

    pthread_cond_signal(&q->not_empty); //wake one sleeping consumer
    pthread_mutex_unlock(&q->mutex);

    return 0;
}

int queue_pop(job_queue_t *q, int *job)
{
    if (q == NULL || job == NULL) {
        return 0;
    }

    /* TODO C8-C10:
     * 1. Lock q->mutex.
     * 2. While the queue is empty and shutdown is false, wait on not_empty.
     * 3. If queue is empty AND shutdown is true, unlock and return 0.
     * 4. Remove the job at head, advance head circularly, decrement count.
     * 5. Signal a producer through not_full.
     * 6. Unlock and return 1.
     */
    pthread_mutex_lock(&q->mutex);

    while (q->count ==0 && q->shutdown == 0) {
        pthread_cond_wait(&q->not_empty, &q->mutex); //sleep if empty
    }

// only exit when both shutdown is 1 and count is 0 otherwise we still have leftover jobs in the buffer that need to be drained first
    if (q->count == 0 && q->shutdown != 0) {
        pthread_mutex_unlock(&q->mutex); //remember to unlock before returning 0 or it deadlocks
        return 0;
    }

    int val = q->jobs[q->head];
    *job =val;
    q->head = (q->head+ 1) % QUEUE_CAPACITY; //use = (use + 1) % MAX
    q->count--; // decrement count inside lock
    // head tail and count all updated under same mutex

    pthread_cond_signal(&q->not_full); //wake producer waiting for space
    pthread_mutex_unlock(&q->mutex);

    return 1;
}

void queue_shutdown(job_queue_t *q)
{
    if (q == NULL) {
        return;
    }

    /* TODO C11-C12:
     * 1. Lock q->mutex.
     * 2. Set q->shutdown = 1.
     * 3. Wake ALL consumers sleeping on not_empty.
     * 4. Unlock q->mutex.
     *
     * Explain in answers.txt why broadcast(), rather than signal(),
     * is appropriate during shutdown.
     */
    pthread_mutex_lock(&q->mutex);
    q->shutdown= 1; //set under lock so no lost wakeup race between check and cond_wait
// using broadcast instead of signal so all 4 printer threads wake up when count is 0 instead of just one waking up and the rest hanging in join forever
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full); //in case a producer is stuck too
    pthread_mutex_unlock(&q->mutex);
}
