# XLog Replay: Concurrently Replay XLog with un-multi-thread-safe XLog Replay Code

## 1. Multi-Process and Multi-Thread

The PostgreSQL is a multi-process model. Each process only has one thread, and of course, the code is not 
multi thread safe.

As we are using Thrift to implement the RPC server, the RPC server is a multi-thread model. Each RPC request
will arise a new thread to handle the request. And these requests usually need 
to replay XLogs. So we need to make the XLog replay code multi-thread safe or to bypass this problem.

## 2. Why make PostgreSQL multi-threads-safe is a BAD idea?

PostgreSQL is a very heavy project. It is not enough to just revise the file that you needed.

For example, if you want to make the XLog replay code multi-thread safe, you need to revise the XLog replay code, and 
you need to revise the XLog reader code, then you need to revise the storage manager code, then further, you need to revise the
file access code, and so on. Finally, you got all these code revised, but the problem comes, the mutex and lock
are also not multi-thread safe! So, based on my experience, it is almost impossible to make PostgreSQL multi-thread safe.

## 3. Solution from NeonDB

NeonDB is developed to be compatible with PostgreSQL. And in its storage nodes, it indeed
uses PostgreSQL's XLog replay code to regenerate the pages from XLogs. And its storage engine are
implemented with Rust language, which is a multi-thread model. 

What NeonDB did is to let the XLog replay as a standalone process. And the storage engine will 
communicate with the XLog replay process via IPC (pipe). So, multiple threads in the storage engine
can send requests to the XLog replay process concurrently. And the XLog replay process will receive these
requests one by one, and replay the XLogs in the order of the requests. In this way, NeonDB bypasses 
the multi-thread problem, because indeed, there is at most one XLog been replayed at the same time.

## 4. Our Solution

Our solution is similar to NeonDB's solution. We also let the XLog replay as a standalone process. But we arise 
several XLog replay processes, and each process will replay XLogs independently. 

When there are multiple XLog replay requests, the RPC server will send these requests to different XLog replay processes,
and XLogs can be replayed concurrently. In this way, we bypass the multi-thread problem, because indeed, there is at most one XLog been replayed at the same time in each XLog replay process.
And at the same time, we can replay multiple XLogs at the same time.

