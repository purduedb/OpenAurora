# Concurrency Control in Disaggregated Database : Multi-Version Page Store

## 1. What is Multi-Version Page Store?

Multi-Version Page Store is a page store that can store multiple versions of the same page.

In the traditional database, there is only one version of the page. But in the disaggregated database, there are multiple versions of the same page. For example, in the disaggregated database, the page version is not only determined by the pageID, but also determined by the LSN. So, there are multiple versions of the same page in the disaggregated database.

But it is easily to fail to distinguish the difference between multi-version pages in disaggregated database and multi-version tuples in traditional database. 
In the traditional database, the multi-version tuples are used to implement the concurrency control of multiple concurrent local transactions. But in the disaggregated database, the multi-version pages are used to support the concurrent multiple compute nodes.

For example, the PostgreSQL supports MVCC (Multi-Version Concurrency Control). In PostgreSQL, the multi-version tuples are used to implement the MVCC, which means that one page can store the same tuple multiple times with different versions. And the PostgreSQL will use the tuple version to implement the concurrency control among different local transactions.
But the multi-version pages in disaggregated database are used to support the concurrent multiple compute nodes. For example, in a multi-version page store, it could store one page several times with different versions. And each version of the page will contain multiple versions of one tuple.



## 2. Why Multi-Version Pages Are Needed?

The multi-version pages are needed because the disparity among the compute nodes status. 

As there are multiple compute nodes in the disaggregated database, the compute nodes may have different status. For example, some compute nodes may be slower than others, and some compute nodes may be down. So, the compute nodes may have different LSNs. Let we assume that there are two compute nodes, and the LSN of the first compute node is 100, and the LSN of the second compute node is 200. 
And if the storage node only store the page version for LSN 100, then the second compute node will not be able to read the latest updates. 
And if the storage node only store the page version for LSN 200, then the first compute node could see the "future" updates and got confused.

So, the solution is to store multiple versions of the same page. For example, the storage node could store the page version for LSN 100 and LSN 200. Then the first compute node could read the page version for LSN 100, and the second compute node could read the page version for LSN 200. In this way, the storage node could support the concurrent multiple compute nodes.


## 3. How to Implement Multi-Version Page Store?

In our implementation, we use the LogIndex as the index of page versions of each pageID. The details could be found at "What is LogIndex?" section. 

Here, we want to talk about how to save these different page versions of the same page. In our implementation, we use the KV store to save these page versions. 

For the key, we use the pageID + LSN as the key. For the value, we use the page content as the value.
So the rpc server will firstly get pageID target version using the LogIndex, and then get the page content from the KV store with the combination of pageID and LSN.
