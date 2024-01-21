# What is Metadata Cache? Why does it important?

## 1. What is Metadata Cache?


The most popular DBMS, like PostgreSQL and MySQL are designed based on local disk. During interacting with file system, the database need to access metadata first, which usually include the number of relations page to help query engine determine the new page's location, whether relation exists or not to deal with the possible truncation operations, page creation and modification LSNs to determine which is the last transaction to modify this page and so on. Since accessing local files' metadata is cheap, frequently accessing local files' metadata is not an obvious overhead. However, for the disaggregaed architecture, the files are detached to the remote servers, so the previous cheap accessing files metadata operations become expensive RPC requests. 


## 2. Common Solutions from PolarDB and Our Approach

To resolve this issue, the popular disaggregated DBMS products introduce meta data caching strategies. For example, the open-source PolarDB for PostgreSQL implemented a rel_size_cache to cache relation metadata, page lsn data and db states in its compute nodes. In our disaggregated architectures, we also implemented a rel_cache for compute nodes to temporary cache relation size and relation existence. With this rel_cache, compute nodes could cache hot relation metadata in local memory and avoid frequently accessing remote storage for these information.

## 3. The Implementation of Metadata Cache

### 3.1. The Structure of Metadata Cache

The metadata cache is a hashmap, which is implemented by boost library. The key of the hashmap is the relation ID, and the value is the metadata of the relation.

### 3.2 The Size of Metadata Cache

As the metadata cache will be stored in the compute nodes' memory, we should limit the size of the metadata cache. The size of the metadata cache is defined by the macro `REL_CACHE_SIZE`. The default value of `REL_CACHE_SIZE` is 200MB. The number of buckets is a prime number, which is used to avoid hash collision.

### 3.3 Replace Policy of Metadata Cache

When the metadata cache is full, we should replace some metadata with new metadata. The default replace policy is LRU (Least Recently Used). The metadata cache will record the last access time of each metadata. When the metadata cache is full, the metadata with the oldest access time will be replaced.

### 3.4 The Startup of Metadata Cache

At the beginning, the metadata cache is empty, and the metadata will be fetched from the storage node when the compute node needs it. 
The metadata will stay in the metadata cache until it is replaced by other metadata, or the compute node's query engine informs that
this metadata is outdated, and use a new metadata to replace this stale one.