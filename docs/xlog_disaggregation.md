# How does Compute Node directly commit XLogs  to Storage Nodes?


## 1. The common commit path

When compute node processing a transaction, it will generate
XLogs. The XLogs will be generated in the memory, and will be
flushed to the disk when there is enough XLog page in the memory.

## 2. The Common Commit Path in Popular Cloud-Native Database

NeonDB: NeonDB will generate XLogs in the compute nodes, and then
flush the XLogs to the local  storage. After that, it will reuse PostgreSQL's
replication protocol to send the XLogs to the storage nodes.

As NeonDB did, there are two time-consuming steps in the common commit path:

1. Flush XLogs to the local storage.

2. Send XLogs to the storage nodes via Network.

We aim to reduce the unnecessary local disk XLog flush step.

## 3. Our Approach 

We will generate XLogs in the compute nodes memory. But when compute nodes try to
flush XLogs to the local storage as PostgreSQL did, we will map the local disk to
the storage node. So, the PostgreSQL logic will work as usual, but the XLogs will
indeed be sent to the storage nodes, instead of being flushed to the local disk.

## 4. How to map the local disk to the storage node?

We map XLog file control related functions to the RPC function. For example, the open()
function will be mapped to rpc_open(), the write() function will be mapped to rpc_write(),
and the fsync() function will be mapped to rpc_fsync().

So, when compute nodes use open() function to open or create a file, it indeed uses rpc_open()
to open or created a file in the storage node. And the storage node will return a file descriptor (integer)
to the compute node. The compute node will cache this file descriptor with virtual file descriptor layer.
When the compute node uses write() function to write data to the file, it will use rpc_write() to send the 
corresponding file descriptor and data to the storage node. The storage node will use the file descriptor to
open the previously opened XLog file and append the new coming XLog to this file.

Then the storage node will return "OK" to compute node via RPC response. The rpc_write function will return
"OK" to XLog generator, and finally, the XLog generator will realise its XLog has been successfully flushed to 
the local disk (in fact, it has been sent to the storage node), and commit the transaction.
