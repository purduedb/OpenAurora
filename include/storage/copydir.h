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

#ifdef __cplusplus
}
#endif

#endif							/* COPYDIR_H */
