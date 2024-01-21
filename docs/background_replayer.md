# Background XLog Replayer

## 1. What is XLog Background Replayer?

As we mentioned in the `Introduction to XLog`, the XLog is used to record the changes of the database.
For the most cases, the XLog will not be replayed immediately, instead, it will be saved in to the disk, and 
record this XLog's LSN in the LogIndex. 

When there is a GetPage@LSN request arrives, the rpc server will check whether the suitable page version exists in the RocksDB.
If there is no suitable page version, the rpc server will fetch the latest page version and replay the XLogs to get the suitable page version.
However, it will inevitably cause a performance degradation. So we introduce the XLog Background Replayer to replay the XLogs in the background asynchronously.


## 2. How does XLog Background Replayer work?

There is a LSN list for each page saved in the LogIndex. In the header of each page's LSN list, there is metadata saved. The 
metadata contains the minimum LSN and the maximum LSN of this page. The XLog Background Replayer will check the minimum LSN and the maximum LSN of each page, and 
the newest LSN of available page version in RocksDB.

The XLog Background Replayer is a thread. Usually, there will have several XLog Background Replayer threads running in the background.
It will periodically check whether there is a page version that can be replayed. If there is, the XLog Background Replayer will fetch the latest page version from
the RocksDB, and also fetch several XLogs from the storage. Then it will replay these XLogs to generate the page version. 

Finally, it will inserted the new page version into the RocksDB, and also update the LogIndex.