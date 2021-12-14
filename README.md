# OpenAurora: An Open-Source Cloud-Native Database System

Cloud-native databases are designed from the ground up to take the full advantage of the cloud infrastructure to provide a fully managed SQL database service in the cloud, i.e., database-as-a-service (DBaaS), to achieve the best elasticity, on-demand scaling, cost-efficiency, and etc. Examples include Amazon Aurora and Microsoft Socrates. However, a pain point is that existing cloud-native databases are all commercial products that are closed source. This prevents many possible optimizations from academia to improve and evaluate cloud-native databases. OpenAurora is an open-source version of Amazon Aurora, which is the first cloud-native OLTP database. Our hope is to provide an open platform for our database community to enable more research and optimizations in cloud-native database systems.

## Features
* Resource disaggregation
* Shared-storage architecture
* Log-is-the-database
* Asynchronous processing
* Optimized for new hardware, e.g., RDMA and 3D Xpoint
* Sinle-master-multi-replica

## Roadmaps

### Expected to finish in 02/15/2022.

### MileStone 1
* Decouple the log-replay functions to a new module, called "Replay Module".
* Truncate the original page data write path (Execution Engine -> Shared Buffer -> Hard Disk).
* Create a new page data write path (XLog -> Replay Module -> Shared Buffer -> Hard Disk).

### MileStone 2
* Decouple compute from storage
* Compute node services includs: SQL parser, SQL optimizer, transaction manager, access method, execution engine, buffer memoger, replication module, storage manager API.
* Storage node services include: storage manager, replay module, vaccum service, PostgreSql storage engine.


### MileStone 3
* Deploy a K/V store (RocksDB) inside PostgreSql.
* Transform InitDB function to initialize DB environment in K/V store.
* Implement a buffer manager in storage node.
* Replay module replays the XLog and store the pages data into K/V store.
* Storage manager read page data from K/V Store.

### MileStone 4
* Replace metadata to K/V store.
* Apply a page MVCC in this project.

### MileStone 5
* Support multi-user: one primary node, several replica

### MileStone 6
* Implement distributed storage layer.
* Implement a gossip protocol to guarantee consistency among different storage nodes.
* Deploy a load balancer to balancer storage node workload. 

## Building

* Running configure script to choose the options and configure source tree.
```bash
./configure --prefix=$project_dir
```
* Type 
```bash
make 
```
to build the project
* Type ```bash
make install
```to install OpenAurora

## How to run
* Running commands is inherited from PostgreSQL



