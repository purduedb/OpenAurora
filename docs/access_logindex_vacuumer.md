# Background LogIndex Vacuumer Implementation Details

## 1. What is LogIndex Vacuumer?

As we mentioned in the `Introcution to LogIndex`, LogIndex is a concurrent hashmap, which maps 
from pageID to LSN list. The LSN list is a linked list, which stores the LSNs of the page.
However, this linked list will be very long if there is no vacuumer, and thus cause a performance
degradation. So this Vacuumer is used to clean the LSN list.


## 2. Which LSNs should be cleaned?

If there is a page version that will never be used by any 
compute nodes, it could be vacuumed. For example, if there is a page version whose LSN is 100,
and the minimum LSN of all the compute nodes is 200, then this page version will never be used.
So it could be vacuumed.

## 3. How to vacuum the LSNs?
The compute nodes will notify the LSN status of itself periodically. The storage node will 
record the minimum LSN of all the compute nodes. Then the storage node will compare the minimum
LSN will all pageID's LSN list in the LogIndex. If the linked LSN list's one node's LSNs are 
all smaller than the minimum LSN, then this node will be vacuumed, which is done by deleting this node.

## 4. How to implement the Vacuumer?
Key word: multi-theading, periodically check, concurrent hashmap, linked list, lock-free
* multi-threading: There will be several vacuumer threads running in the background. Each thread will check the LSN list of one page.
* periodically check: Each thread will check the LSN list periodically. The period is 1 second.
* concurrent hashmap: The LogIndex is a concurrent hashmap, which is implemented with the `concurrent_hashmap` crate.
* linked list: The LSN list is a linked list, which is implemented with the `linked_list` crate.
* lock-free: The `concurrent_hashmap` crate is lock-free. And the `linked_list` crate is also lock-free.

## 5. How to temporarily disable the Vacuumer?
### 5.1 Why need to temporarily disable the Vacuumer?
The Vacuumer will cause a performance degradation. So we need to temporarily disable the Vacuumer when the performance is critical.
### 5.2 How to temporarily disable the Vacuumer?
The Vacuumer will check the `vacuum_enabled` flag periodically. If the `vacuum_enabled` flag is false, the Vacuumer will not vacuum any LSN list.
The `vacuum_enabled` flag is set by the `set_vacuum_enabled` RPC request. The `set_vacuum_enabled` RPC request is sent by the `set_vacuum_enabled` command in the `pg_neon` command line tool.
