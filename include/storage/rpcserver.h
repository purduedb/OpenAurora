/*-------------------------------------------------------------------------
 *
 * rpcserver.h
 *	
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/rpcserver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPCSERVER_H
#define RPCSERVER_H

#ifdef __cplusplus
extern "C" {
#endif

void RpcServerLoop(void);

#ifdef __cplusplus
}
#endif

#endif               /* RPCSERVER_H */