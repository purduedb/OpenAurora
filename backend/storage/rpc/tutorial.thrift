/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

# Thrift Tutorial
# Mark Slee (mcslee@facebook.com)
#
# This file aims to teach you how to use Thrift, in a .thrift file. Neato. The
# first thing to notice is that .thrift files support standard shell comments.
# This lets you make your thrift file executable and include your Thrift build
# step on the top line. And you can place comments like this anywhere you like.
#
# Before running this file, you will need to have installed the thrift compiler
# into /usr/local/bin.

/**
 * The first thing to know about are types. The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  i8 (byte)   Signed 8-bit integer
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 * Did you also notice that Thrift supports C style comments?
 */

// Just in case you were wondering... yes. We support simple C comments too.

/**
 * Thrift files can namespace, package, or prefix their output in various
 * target languages.
 */

namespace cl tutorial
namespace cpp tutorial
namespace d tutorial
namespace dart tutorial
namespace java tutorial
namespace php tutorial
namespace perl tutorial namespace haxe tutorial
namespace netstd tutorial


typedef i32 _File

typedef i32 _Flag;

/**
 * Data structures in thrift do not support unsigned int32, so we use i64 instead.
 * Remember to change the struct being used.
 */
typedef i64 _Oid

typedef i64 _Off_t

typedef binary _Path

typedef binary _Page

struct _Stat_Resp {
	1: i32 _result,
	2: i32 _stat_mode,
}

struct _XLog_Init_File_Resp {
	1: i32 _fd,
	2: i32 _use_existent,
}

struct _Smgr_Relation {
	1: _Oid _spc_node,
	2: _Oid _db_node,
	3: _Oid _rel_node,
	4: i32 _backend_id,
}
/**
 * Ahh, now onto the cool part, defining a service. Services just need a name
 * and can optionally inherit from another service using the extends keyword.
 */
service DataPageAccess {

  /**
   * A method definition looks like C code. It has a return type, arguments,
   * and optionally a list of exceptions that it may throw. Note that argument
   * lists and exception lists are specified using the exact same syntax as
   * field lists in struct or exception definitions.
   */

   _Page ReadBufferCommon(1:_Smgr_Relation _reln, 2:i32 _relpersistence, 3:i32 _forknum, 4:i32 _blknum, 5:i32 _readBufferMode, 6:i64 _lsn),

   _Page RpcMdRead(1:_Smgr_Relation _reln, 2:i32 _forknum, 3:i64 _blknum, 4:i64 _lsn),

   i32 RpcMdNblocks(1:_Smgr_Relation _reln, 2:i32 _forknum, 3:i64 _lsn),

   i32 RpcMdExists(1:_Smgr_Relation _reln, 2:i32 _forknum, 4:i64 _lsn),

   void RpcMdCreate(1:_Smgr_Relation _reln, 2:i32 _forknum, 3:i32 _isRedo, 4:i64 _lsn),

   void RpcMdExtend(1:_Smgr_Relation _reln, 2:i32 _forknum, 3:i32 _blknum, 4:_Page _buff, 5:i32 skipFsync, 6:i64 _lsn), 
   
   void RpcTruncate(1:_Smgr_Relation _reln, 2:i32 _forknum, 3:i32 _blknum, 4:i64 _lsn),

   /*following is fd.c interface*/
   void RpcFileClose(1:_File _fd),

   void RpcTablespaceCreateDbspace(1:_Oid _spcnode, 2:_Oid _dbnode 3:bool isRedo),

   _File RpcPathNameOpenFile(1:_Path _path, 2:_Flag _flag),

   i32 RpcFileWrite(1:_File _fd, 2:_Page _page, 3:i32 _amount, 4:_Off_t _seekpos, 5:i32 _wait_event_info),

   _Path RpcFilePathName(1:_File _fd),

   _Page RpcFileRead(1:_File _fd, 2:i32 _amount, 3:_Off_t _seekpos, 4:i32 _wait_event_info),

   i32 RpcFileTruncate(1:_File _fd, 2:_Off_t _offset),

   _Off_t RpcFileSize(1:_File _fd),

   i32 RpcFilePrefetch(1:_File _fd, 2:_Off_t _offset, 3:i32 _amount, 4:i32 wait_event_info),

   void RpcFileWriteback(1:_File _fd, 2:_Off_t _offset, 3:_Off_t nbytes, 4:i32 wait_event_info),

   i32 RpcUnlink(1:_Path _path),

   i32 RpcFtruncate(1:_File _fd, 2:_Off_t _offset),

   _Page RpcInitFile(1:_Path _path),

   _File RpcOpenTransientFile(1:_Path _filename, 2:i32 _fileflags),

   i32 RpcCloseTransientFile(1:_File _fd),

   _Page Rpcread(1:_File _fd, 2:i32 size),

   i32 Rpcwrite(1:_File _fd, 2:_Page _page, 3:i32 size),
   
   i32 RpcFileSync(1:_File _fd, 2:i32 _wait_event_info),

   _Page RpcPgPRead(1:_File _fd, 2:i32 _seg_bytes, 3:_Off_t _start_off),

   i32 RpcPgPWrite(1: _File _fd, 2:_Page _page, 3:i32 _amount, 4: _Off_t _offset),

   /* linux close() function */
   i32 RpcClose(1: _File _fd),

   i32 RpcBasicOpenFile(1: _Path _path, 2: i32 _flags),

   i32 RpcPgFdatasync(1: _File _fd),

   i32 RpcPgFsyncNoWritethrough(1: _File _fd),

   i32 RpcLseek(1:i32 _fd, 2:_Off_t _offset, 3:i32 _flag),

   _Stat_Resp RpcStat(1:_Path _path),

   i32 RpcDirectoryIsEmpty(1:_Path _path),

   i32 RpcCopyDir(1:_Path _src, 2:_Path _dst),

   i32 RpcPgFsync(1:i32 _fd),

   i32 RpcDurableUnlink(1:_Path _fname, 2:i32 _flag),

   i32 RpcDurableRenameExcl(1:_Path _oldFname, 2:_Path _newFname, 3:i32 _elevel),

   i32 RpcXLogWrite(1: _File _fd, 2:_Page _page, 3:i32 _amount, 4: _Off_t _offset, 5: list<i64> _xlblocks, 6: i32 _blknum, 7: i32 _idx, 8: i64 _lsn),

   _XLog_Init_File_Resp RpcXLogFileInit(1:i64 _logsegno, 2:i32 _use_existent, 3:i32 _use_lock),
  
   /**
    * This method has a oneway modifier. That means the client only makes
    * a request and does not listen for any response at all. Oneway methods
    * must be void.
    */
   oneway void zip()

}

/**
 * That just about covers the basics. Take a look in the test/ folder for more
 * detailed examples. After you run this file, your generated code shows up
 * in folders with names gen-<language>. The generated code isn't too scary
 * to look at. It even has pretty indentation.
 */
