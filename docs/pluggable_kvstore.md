# Pluggable KVStore: Working with your own KVStore

## 1. Pluggable KVStore

As we mentioned in the "Concurrency Control in Disaggregated Database : Multi-Version Page Store" section, we use the KV store to save the page versions. In this project, we provide a pluggable KVStore interface. You can implement your own KVStore and use it in this project.

## 2. How to plug your own KVStore in?

All KVstore that implemented the following five interfaces could be plugged in:
* kvstore_init()
* kvstore_put()
* kvstore_get()
* kvstore_delete()
* kvstore_destroy()

## 3. The Implementation of Our Own KVStore

This KV-store has three layers. 

### 3.1 Index Layer

The first layer is an index layer. It's implemented by a concurrent hashmap, which using pthread_rw_lock to avoid threads conflictions. It will interact with caller functions.

There are three APIs to communicate with outer programs: GetValue(), InsertKVPair(), DeleteValue().

GetValue() will receive a key-value pair (pageID->pageContnet). But this layer will only store page's metadata in this map, which key is pageID and value is pageOffset in the disk files. The actual pages will be hold by buffer layer and disk layer.

This layer will works as a pure index. For the GetValue() requests, it will check whether this pageID exists in index, if not, just return false. Otherwise, it will find target page's offset, and try to fetch page from buffer layer.

This layer won't interact with disk layer. It will try to fetch all pages from buffer, if the target pages are not cached in buffer, the buffer layer will automatically fetch them from disk.

InsertKVPair() aims to store a new page or update an existed page in KV-store. If the coming page is existed, it will transfer this page to buffer pool to update this page. If the coming page is not existed, it will let the buffer layer allocate a file space (logical allocation) for this page.
After that, the index layer will store the pageID->pageOffset pair in its index, and let buffer pool caches this page.


### 3.2 Bufferr Pool Layer


Buffer pool layer will temporarily cache the hot page in the memory. It's implemented upon a LRU cache.

From the LRU layer perspective, this buffer pool layer works
as a file system. The index layer will send the newly coming pages to buffer pool layer, and buffer pool layer will allocate space to accommodate these pages by evicting
cold pages. And the index layer will read pages from buffer pool layer. If the target page is not cached in buffer pool layer, it will fetch this page from disk layer.

### 3.3 Disk Manager Layer


Disk manager layer's primary responsibility is to store pages in the specific offset, and fetch pages from the specific offset.
And disk manager will also provide available offset to accommodate new pages. Disk manager will automatically divide the storage into several segments, and each
segment space is 16MB. Disk manager will also reuse the space of deleted pages. It will
link all deleted pages' offsets into freelist, and reuse these offsets to store new pages.


