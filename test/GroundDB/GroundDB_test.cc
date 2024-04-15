#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/GroundDB/rdma_server.hh"

namespace mempool {
    TEST(FreeList_Test, PopFront){
        auto fl = new FreeList();
        fl->init();
        uint8_t page1[BLCKSZ], page2[BLCKSZ];
        KeyType pid1 = {0, 0, 0, 0, 1}, pid2 = {0, 0, 0, 0, 2};
        PageMeta pm1 = {page1, &pid1}, pm2 = {page2, &pid2}, pm3 = {page1, &pid2}, *tmp;

        fl->push_back(&pm1);
        fl->push_back(&pm2);
        tmp = fl->pop_front();
        ASSERT_EQ(tmp, &pm1);
        tmp = fl->pop_front();
        ASSERT_EQ(tmp, &pm2);
        tmp = fl->pop_front();
        ASSERT_EQ(tmp, nullptr);
        fl->push_back(&pm3);
        tmp = fl->pop_front();
        ASSERT_EQ(tmp, &pm3);
        tmp = fl->pop_front();
        ASSERT_EQ(tmp, nullptr);
    }
}

namespace DSMEngine {
    TEST(LRUCache_Test, LRU_Policy){
        uint8_t page[4000];
        mempool::PageMeta pm[2000];
        auto fl = new mempool::FreeList();
        fl->init();
        for(int i = 0; i < 2000; i++)
            fl->push_back(&pm[i]);

        auto lru = DSMEngine::NewLRUCache(2000, fl);

        Cache::Handle* e;
        int cache_hit;
        for(int i = 0; i < 2000; i++){
            e = lru->LookupInsert((KeyType){0, 0, 0, 0, i}, nullptr, 1, nullptr);
            ((mempool::PageMeta*)e->value)->page_addr = &page[i];
            lru->Release(e);
        }
        for(int i = 2000; i < 3000; i++){
            e = lru->LookupInsert((KeyType){0, 0, 0, 0, i - 1000}, nullptr, 1, nullptr);
            ((mempool::PageMeta*)e->value)->page_addr = &page[i];
            lru->Release(e);
        }
        cache_hit = 0;
        for(int i = 0; i < 1000; i++){
            e = lru->Lookup((KeyType){0, 0, 0, 0, i});
            if(e != nullptr){
                ASSERT_EQ(((mempool::PageMeta*)e->value)->page_addr, &page[i]);
                lru->Release(e);
                cache_hit++;
            }
        }
        ASSERT_GE(cache_hit, 600);
        cache_hit = 0;
        for(int i = 1000; i < 2000; i++){
            e = lru->Lookup((KeyType){0, 0, 0, 0, i});
            if(e != nullptr){
                ASSERT_EQ(((mempool::PageMeta*)e->value)->page_addr, &page[i + 1000]);
                lru->Release(e);
                cache_hit++;
            }
        }
        ASSERT_GE(cache_hit, 600);

        for(int i = 3000; i < 4000; i++){
            e = lru->LookupInsert((KeyType){0, 0, 0, 0, i}, nullptr, 1, nullptr);
            ((mempool::PageMeta*)e->value)->page_addr = &page[i];
            lru->Release(e);
        }
        cache_hit = 0;
        for(int i = 0; i < 1000; i++){
            e = lru->Lookup((KeyType){0, 0, 0, 0, i});
            if(e != nullptr){
                ASSERT_EQ(((mempool::PageMeta*)e->value)->page_addr, &page[i]);
                lru->Release(e);
                cache_hit++;
            }
        }
        ASSERT_LE(cache_hit, 400);
    }

    TEST(ThreadPool_Test, MultiThreadingAdd) {
        auto thrd_pool = new DSMEngine::ThreadPool();
        thrd_pool->SetBackgroundThreads(5);
        std::atomic<int> counter;
        counter.store(0);
        for(int i = 0; i < 10; i++){
            std::function<void(void*)> func = [&counter, i](void* args){
                for(int j = 0; j < 1000; j++)
                    counter++;
            };
            thrd_pool->Schedule(std::move(func), nullptr, i % 5);
        }
        thrd_pool->JoinThreads(true);
        ASSERT_EQ(counter.load(), 1000 * 10);
    };

    class RDMA_Manager_Test : public testing::Test {
    public:
    protected:
        void SetUp() override {
            uint32_t tcp_port = 19843;
            struct DSMEngine::config_t config = {
                    NULL,  /* dev_name */
                    tcp_port, /* tcp_port */
                    1,	 /* ib_port */
                    1, /* gid_idx */
                    0,
                    0};
            rdma_mg = RDMA_Manager::Get_Instance(&config);
            rdma_mg->Mempool_initialize(DataChunk, INDEX_BLOCK, 0);
        }
        DSMEngine::RDMA_Manager* rdma_mg;
    };

    TEST_F(RDMA_Manager_Test, LocalAllocation) {
        ibv_mr mr{};
        rdma_mg->Allocate_Local_RDMA_Slot(mr, DataChunk);
        ASSERT_EQ(rdma_mg->name_to_mem_pool.at(DataChunk).size(), 1);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}