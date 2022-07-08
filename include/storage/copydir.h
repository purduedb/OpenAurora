/*-------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

#ifdef __cplusplus
extern "C" {
#endif

extern void copydir(char *fromdir, char *todir, bool recurse);

extern void copy_file(char *fromfile, char *tofile);

extern void copy_dir_rpc_or_local(char *_src, char* _dst);

extern int stat_rpc_or_local(char* _path, struct stat * _stat);

extern int directory_is_empty_rpc_or_local(char* _path);

#ifdef __cplusplus
}
#endif

#endif							/* COPYDIR_H */
