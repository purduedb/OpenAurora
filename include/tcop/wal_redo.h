#ifndef PG_WAL_REDO_H
#define PG_WAL_REDO_H

extern void WalRedoMain(int argc, char *argv[],
                        const char *dbname,
                        const char *username);


#endif //PG_WAL_REDO_H
