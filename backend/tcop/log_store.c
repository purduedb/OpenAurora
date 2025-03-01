#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include "postgres.h"
#include "tcop/tcopprot.h"
#include "tcop/rdma.h"





#define SERVER_PORT 5555
#define XLOG_SIZE 256
#define LSN_OFFSET 0
#define FLUSH_LSN_OFFSET 8
#define RING_BUFFER_OFFSET 16
#define RING_BUFFER_SIZE (128 * 1024 * 1024)  // 128MB

struct xlog_entry {
    uint64_t lsn;
    char data[XLOG_SIZE];
};

atomic_uint_least64_t current_lsn = 0;
atomic_uint_least64_t flush_lsn = 0;
pthread_mutex_t lsn_mutex = PTHREAD_MUTEX_INITIALIZER;
FILE *disk_file;
volatile int keep_running = 1;

void sigint_handler(int signo) {
    keep_running = 0;
}




void *background_process(void *arg) {
    struct resources *res = (struct resources *)arg;
    while (keep_running) {
        uint64_t lsn_to_flush = atomic_load(&current_lsn);

        if (lsn_to_flush > atomic_load(&flush_lsn)) {
            size_t flush_size = (lsn_to_flush - atomic_load(&flush_lsn)) * sizeof(struct xlog_entry);
            size_t start_pos = (atomic_load(&flush_lsn) * sizeof(struct xlog_entry)) % RING_BUFFER_SIZE;

            if (start_pos + flush_size <= RING_BUFFER_SIZE) {
                if (fwrite(res->buf + RING_BUFFER_OFFSET + start_pos, 1, flush_size, disk_file) != flush_size) {
                    fprintf(stderr, "Error writing to file: %s\n", strerror(errno));
                }
            } else {
                size_t first_part = RING_BUFFER_SIZE - start_pos;
                if (fwrite(res->buf + RING_BUFFER_OFFSET + start_pos, 1, first_part, disk_file) != first_part) {
                    fprintf(stderr, "Error writing first part to file: %s\n", strerror(errno));
                }
                if (fwrite(res->buf + RING_BUFFER_OFFSET, 1, flush_size - first_part, disk_file) != (flush_size - first_part)) {
                    fprintf(stderr, "Error writing second part to file: %s\n", strerror(errno));
                }
            }
            fflush(disk_file);
            fsync(fileno(disk_file));

            atomic_store(&flush_lsn, lsn_to_flush);
            *((uint64_t *)(res->buf + FLUSH_LSN_OFFSET)) = htobe64(atomic_load(&flush_lsn));

            printf("Flushed data to testing_disk.txt. New Flush-LSN: %lu\n", atomic_load(&flush_lsn));

            FILE *flush_info = fopen("flush_info.txt", "a");
            if (flush_info) {
                fprintf(flush_info, "Flushed LSN range: %lu - %lu\n", 
                        atomic_load(&flush_lsn) - (flush_size / sizeof(struct xlog_entry)), 
                        atomic_load(&flush_lsn));
                fclose(flush_info);
            }
        }

        usleep(100000);  // Check timing
    }
    return NULL;
}

int LogStoreMain(int argc, char *argv[], const char *dbname, const char *username) 
{
    struct resources res;
    struct sockaddr_in addr;
    resources_init(&res);

    disk_file = fopen("testing_disk.txt", "w+");
    if (disk_file == NULL) {
        fprintf(stderr, "Failed to open testing_disk.txt\n");
        return 1;
    }

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        fprintf(stderr, "Failed to create socket\n");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(SERVER_PORT);

    if (bind(server_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Failed to bind to port\n");
        return 1;
    }

    if (listen(server_sock, 1) < 0) {
        fprintf(stderr, "Failed to listen on socket\n");
        return 1;
    }

    printf("LogStore listening on port %d\n", SERVER_PORT);

    res.sock = accept(server_sock, NULL, NULL);
    if (res.sock < 0) {
        fprintf(stderr, "Failed to accept connection\n");
        return 1;
    }

    printf("TCP connection accepted\n");

    // RDMA setup
    if (resources_create(&res) != 0 || connect_qp(&res) != 0) {
        fprintf(stderr, "Failed to create RDMA resources or connect QPs\n");
        return 1;
    }

    printf("RDMA connection established.\n");
    signal(SIGINT, sigint_handler);

    // Initialize LSN and FLUSH_LSN
    *((uint64_t *)res.buf) = 0;
    *((uint64_t *)(res.buf + FLUSH_LSN_OFFSET)) = 0;

    pthread_t bg_thread;
    pthread_create(&bg_thread, NULL, background_process, &res);

    while (keep_running) {
    uint64_t new_lsn = be64toh(*((uint64_t *)res.buf));

    if (new_lsn > atomic_load(&current_lsn)) {
        printf("New LSN detected. Current: %lu, New: %lu\n", atomic_load(&current_lsn), new_lsn);
        
        while (atomic_load(&current_lsn) < new_lsn) {
            uint64_t lsn = atomic_fetch_add(&current_lsn, 1) + 1;
            size_t offset = RING_BUFFER_OFFSET + ((lsn * sizeof(struct xlog_entry)) % RING_BUFFER_SIZE);
            struct xlog_entry *xlog = (struct xlog_entry *)(res.buf + offset);
            
            printf("Processing Xlog: LSN=%lu, Data=%s\n", lsn, xlog->data);
            
            if (strlen(xlog->data) == 0) {
                fprintf(stderr, "Warning: Empty Xlog data for LSN %lu\n", lsn);
            }
        }

        printf("Updated current LSN: %lu\n", atomic_load(&current_lsn));
    }

   

    usleep(1000);
}

    // Cleanup
    pthread_join(bg_thread, NULL);
    resources_destroy(&res);
    close(res.sock);
    close(server_sock);
    fclose(disk_file);

    return 0;
}