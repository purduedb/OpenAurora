# OpenAurora: An Open-Source Cloud-Native Database System

Cloud-native databases are designed from the ground up to take the full advantage of the cloud infrastructure to provide a fully managed SQL database service in the cloud, i.e., database-as-a-service (DBaaS), to achieve the best elasticity, on-demand scaling, cost-efficiency, and etc. Examples include Amazon Aurora and Microsoft Socrates. However, a pain point is that existing cloud-native databases are all commercial products that are closed source. This prevents many possible optimizations from academia to improve and evaluate cloud-native databases. OpenAurora is an open-source version of Amazon Aurora, which is the first cloud-native OLTP database. Our hope is to provide an open platform for our database community to enable more research and optimizations in cloud-native database systems.

## Features
* Resource disaggregation
* Shared-storage architecture
* Log-is-the-database
* Asynchronous processing
* Optimized for new hardware, e.g., RDMA and 3D Xpoint
* Single-master-multi-replica

## Architecture
<img src="OpenAurora-Arch.png" alt="drawing" width="700"/>

## Incoming Progress

### 2022/01/05 - 2022/01/20
** Goal **
* Disaggregate storage layer and compute layer
** What need to do **
* Transform InitDB to create database into a remote server
* Compute node can read server node's meta data and page data
* Use postgresql replication code to connect storage node and compute node
* Compute node services includs: SQL parser, SQL optimizer, transaction manager, access method, execution engine, buffer memoger, replication module, storage manager API.
* Storage node services include: storage manager, replay module, vaccum service, PostgreSql storage engine.
** Potential Risk **
* After decoupled compute layer and storage layer, some services like vacuum service will be temporarily unavailable. This is because they need to cooperate with compute nodes transaction information. It is acceptable these services completion to be delayed.  

### 2022/01/21 - 2022/01/31
**Goal**
* Replace the PostgreSql storage engine with a K/V store
**What need to do**
* Deploy RocksDB engine in PostgreSql storage layer
* Transform meta data writing and reading file into accessing K/V store
* Transform page data writing and reading file into accessing K/V store
* Developing RPC interfaces and related strategy functions


### 2022/02/01 - 2022/02/10
**Goal**
* Replace PostgreSql's tuple level MVCC with page level MVCC
**What need to do**
* Transform PostgreSql storage/page related data structures and functions
* Develop a page MVCC with the K/V store

### 2022/02/11 - 2022/02/17
**Goal**
* Support multi-client: one-primary-several-replicas
**What need to do**
* Develop a load balancer to disseminate query requests to different compute nodes
* Disseminate primary node's write requests to all replicas

### 2022/02/18 - 2022/03/15
**Goal**
* Support distributed storage layer.
**What need to do**
* Deploy distributed K/V storage in a distributed storage environment.
* Implement a gossip protocol to guarantee consistency among different storage nodes.
* Deploy a load balancer to balancer storage node workload. 

## Getting the source code
```bash
git clone https://github.com/px1900/OpenAurora.git
```

## Building

This project is based on [PostgreSql 13.0](https://www.postgresql.org/docs/13/release-13.html "PostgreSQL-13.0") 

* Running configure script to choose the options and configure source tree.
```bash
./configure --prefix=$your_project_dir
```
* Build project
```bash
make 
```

* Install OpenAurora
```bash
make install
```

## Create a database
Using a following command to create a database named <strong> mydb </strong>.
```bash
createdb mydb
```
If you see a message similar to:

```bash
createdb: command not found
```
then OpenAurora was not installed properly. Try calling the command with an absolute path instead:
```bash
$your_project_dir/bin/createdb mydb
```

## Access your database
You can access your database by: 

* Running a PostgreSQL interactive terminal program, which allows you to interactively enter, edit, and execute SQL commands.
* Using an existing graphical frontend tool like pgAdmin or an office suite with ODBC or JDBC support to create and manipulate a database.
* Writing a custom application, using one of the several available language bindings. You can turn to this [Tutorial](https://www.postgresql.org/docs/14/client-interfaces.html "PostgreSQL Client Interfaces").

You can access your created database via interactive terminal with the following command:
```bash
psql mydb
```


