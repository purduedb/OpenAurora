# The Truth of the Disk Disaggregation: Virtual File Descriptor

## 1. What is Virtual File Descriptor?
The virtual file descriptor is a layer between the PostgreSQL and the real file descriptor returned by 
the operating system. 
When the PostgreSQL uses the open() function to open or create a file, it will call the file system to open or create a file, and then the file system will return a file descriptor to the PostgreSQL.
But PostgreSQl won't use this file descriptor directly. Instead, it will use the virtual file descriptor to cache this file descriptor.
And virtual file descriptor will return a virtual file descriptor to the upper layer, and also record a mapping between the virtual file descriptor and the real file descriptor.
When the PostgreSQL uses the write() function to write data to the file, it will send the data and the virtual file descriptor to the virtual file descriptor layer.
Then the virtual file descriptor layer will use the virtual file descriptor to find the real file descriptor, and then use the real file descriptor to write data to the file.
When the PostgreSQL uses the fsync() function to flush data to the disk, it will send the virtual file descriptor to the virtual file descriptor layer.
Then the virtual file descriptor layer will use the virtual file descriptor to find the real file descriptor, and then use the real file descriptor to flush data to the disk.


## 2. Why do we need Virtual File Descriptor?
Usually, there is a limitation of the number of file descriptors that can be opened at the same time by the operating system.

The virtual file descriptor layer can let the PostgreSQL feel that it can open as many files as it wants, but in fact, the real file descriptors are limited.
When the PostgreSQL try to open a new file, but it reaches the maximum number of opened files, the virtual file descriptor layer will close some files that are not used recently, and then return the file descriptor to the PostgreSQL.
When the PostgreSQL try to write data to an indeed already closed file, the virtual file descriptor layer will reopen the file and then write data to the file.

So, in the whole path, the PostgreSQL will not know that the file has been closed and reopened. It will feel that it is writing data to the same file all the time.

## 3. The Relationship between Virtual File Descriptor and Disk Disaggregation

As we mentioned in the `How does Compute Node directly commit XLogs  to Storage Nodes?` section, we will map the local disk to the storage node.

But to simplify the implementation, we aim to not change the PostgreSQL's code as much as possible. So, we need to let the PostgreSQL feel that it is writing data to the local disk, but in fact, it is writing data to the storage node.
So, we decide to revise the virtual file descriptor layer to let it send the data to the storage node instead of writing data to the local disk.

When the PostgreSQL uses the open() function to open or create a file, the virtual file descriptor layer will 
map this command to rpc_open(), and let the storage node to open or create a file, and then the storage node will return a file descriptor to the virtual file descriptor layer.
The virtual file descriptor layer will cache this file descriptor, and then return a virtual file descriptor to the PostgreSQL.

When the PostgreSQL uses the write() function to write data to the file, the virtual file descriptor layer will map this command to rpc_write(), and send the data and virtual file descriptor to the storage node.
The storage node will use the file descriptor to open the previously opened XLog file and append the new coming XLog to this file.
Then the storage node will return "OK" to virtual file descriptor layer via RPC response. The virtual file descriptor layer will return "OK" to PostgreSQL, and finally, the PostgreSQL will realise its XLog has been successfully flushed to the local disk (in fact, it has been sent to the storage node).

## 4. Use the Virtual File Descriptor to Simplify the Storage Node Implementation

As we mentioned before, there is usually a limitation of the number of file descriptors that can be opened at the same time by the operating system. So we reuse the virtual file descriptor layer again. 

We deployed PostgreSQL's virtual file descriptor layer in the storage node, and let the virtual file descriptor layer and its lower layer feel that they are communicating with a local PostgreSQL. But in fact, we have
mapped all the interface with RPC. So, the virtual file descriptor layer and its lower layer are communicating with the compute node.
