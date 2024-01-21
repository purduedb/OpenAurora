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

