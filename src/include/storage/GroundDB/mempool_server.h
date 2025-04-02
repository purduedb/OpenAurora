#ifndef DB2_PG_MEMPOOL_SERVER_H
#define DB2_PG_MEMPOOL_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

extern void
MemPoolMain(int argc, char *argv[],
              const char *dbname,
              const char *username);


#ifdef __cplusplus
}
#endif

#endif //DB2_PG_MEMPOOL_SERVER_H