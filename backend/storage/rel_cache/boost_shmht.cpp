#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/unordered_map.hpp> //boost::unordered_map
#include <functional> //std::equal_to
#include <boost/functional/hash.hpp> //boost::hash
#include <iostream>
#include "storage/boost_shmht.h"
#include "port/atomics.h"
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>

using namespace boost::interprocess;

//Note that unordered_map<Key, MappedType>'s value_type is std::pair<const Key, MappedType>,
//so the allocator must allocate that pair.
typedef std::string KeyType;
typedef uint32_t MappedType;
typedef std::pair<std::string, uint32_t> ValueType;
//Typedef the allocator
typedef allocator<ValueType, managed_shared_memory::segment_manager> ShmemAllocator;
//Alias an unordered_map of ints that uses the previous STL-like allocator.
typedef boost::unordered_map< KeyType , MappedType, boost::hash<KeyType>
        ,std::equal_to<KeyType>, ShmemAllocator> MyHashMap;

// free local segment space when process exit
struct shm_segment
{
    managed_shared_memory *segment = NULL;
    ~shm_segment(){ if(segment != NULL){ printf("%s %d, delete segment, pid = %d\n", __func__ , __LINE__, getpid());
            fflush(stdout); delete segment; } }
} shm_segment;

// unlink shared memory when process exit
struct shm_remove
{
    //shm_remove() { std::cout << "remove1" << std::endl; shared_memory_object::remove("MySharedMemory"); }
//    ~shm_remove(){ std::cout << "remove2" << std::endl; shared_memory_object::remove("MySharedMemory"); }
} remover;



MyHashMap *myhashmap;

bool UnorderedHashMapCreate() {
    printf("%s %d, malloc segment local object, pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);

    shared_memory_object::remove("MySharedMemory");
    // Create a new shared memory and attach it
    shm_segment.segment = new managed_shared_memory(create_only, "MySharedMemory", 32*1024*1024);

    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
    //Construct a shared memory hash map.
    //Note that the first parameter is the initial bucket count and
    //after that, the hash function, the equality function and the allocator
    myhashmap = shm_segment.segment->construct<MyHashMap>("MyHashMap") //object name
            ( 1024, boost::hash<std::string>(), std::equal_to<std::string>() //
                    , shm_segment.segment->get_allocator<ValueType>()); //allocator instance
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
    return true;
}

bool UnorderedHashMapAttach() {
    printf("%s %d, malloc segment local object, pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
    shm_segment.segment = new managed_shared_memory(open_only, "MySharedMemory");
//    managed_shared_memory segment(open_only, "MySharedMemory");
    //Construct a shared memory hash map.
    //Note that the first parameter is the initial bucket count and
    //after that, the hash function, the equality function and the allocator
    myhashmap = shm_segment.segment->find_or_construct<MyHashMap>("MyHashMap") //object name
            ( 1024, boost::hash<std::string>(), std::equal_to<std::string>() //
                    , shm_segment.segment->get_allocator<ValueType>()); //allocator instance
    return true;
}

// If key doesn't exist, then insert it
// If key exists then update it
void UnorderedHashMapInsert(char* key, uint32_t value) {

    pg_read_barrier();
    std::string s(key);
//    boost::unordered_map< KeyType , MappedType, boost::hash<KeyType>
//            ,std::equal_to<KeyType>, ShmemAllocator>::iterator iter;
    auto iter = myhashmap->find(s);

    // If this key already exists in hashmap, update it
    if(iter != myhashmap->end()) {
        myhashmap->erase(iter);
        myhashmap->insert(ValueType(s, (uint32_t)value));
        printf("%s %d, key=%s, update the value = %u\n", __func__ , __LINE__, key, value);
        fflush(stdout);
//        iter->second = value;
//        auto newiter = myhashmap->find(s);
//        printf("%s %d, key=%s, after update, the new value = %u\n", __func__ , __LINE__, key, newiter->second);

    } else{ // Not exist, insert it
        printf("%s %d, key=%s, insert the new value = %u\n", __func__ , __LINE__, key, value);
        fflush(stdout);
        myhashmap->insert(ValueType(s, (uint32_t)value));
    }
    pg_write_barrier();
}

bool UnorderedHashMapGet(char* key, uint32_t *result) {
    pg_read_barrier();
    std::string s(key);
    boost::unordered_map< KeyType , MappedType, boost::hash<KeyType>
            ,std::equal_to<KeyType>, ShmemAllocator>::const_iterator iter;
    iter = myhashmap->find(s);
    if(iter != myhashmap->end()) {
        *result = iter->second;
        return true;
    } else {
        return false;
    }
}
