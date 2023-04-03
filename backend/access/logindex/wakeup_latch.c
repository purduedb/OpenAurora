//
// Created by rainman on 2023/3/29.
//
#include <pthread.h>
#include "postgres.h"

#include <stdio.h>
#include <time.h>
#include <sys/stat.h>


pthread_cond_t      start_up_cond  = PTHREAD_COND_INITIALIZER;
pthread_mutex_t     start_up_mutex = PTHREAD_MUTEX_INITIALIZER;

//extern uint64_t RpcXLogFlushedLsn;

void WaitForNewXLog(uint64_t currentLsn, uint32_t wait_usec) {
    // use the currentLsn if we need a cond_wait loop
    int rc;

    rc = pthread_mutex_lock(&start_up_mutex);

    // Calculate the auto-wake-up timestamp
    struct timespec ts;
    struct timeval tp;

    gettimeofday(&tp, NULL);

    ts.tv_sec = tp.tv_sec;
    ts.tv_nsec = tp.tv_usec * 1000;

    ts.tv_sec += (wait_usec*1000) / 1000000000;
    ts.tv_nsec += (wait_usec*1000) % 1000000000;

    rc = pthread_cond_timedwait(&start_up_cond, &start_up_mutex, &ts);

    if (rc == ETIMEDOUT) {
        // some special work
    }

    rc = pthread_mutex_unlock(&start_up_mutex);


}

void WakeupStartupRecovery() {
    int rc = 0;

    rc = pthread_mutex_lock(&start_up_mutex);

    rc = pthread_cond_broadcast(&start_up_cond);

    rc = pthread_mutex_unlock(&start_up_mutex);
}

void WakeupStartupResourceDestroy() {
    pthread_cond_destroy(&start_up_cond);
    pthread_mutex_destroy(&start_up_mutex);
}