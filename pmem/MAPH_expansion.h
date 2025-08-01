#include "murmur3.h"  
#include <cstring>  
#include <random>  
#include <set>  
#include <cstdio>  
#include <stdio.h>  
#include <stdlib.h>  
#include <stdint.h>  
#include <string.h>  
#include <time.h>  
#include <stack>  
#include <unordered_map>  
#include <unordered_set>  
#include <queue>  
#include <chrono> // 添加时间测量支持  
#include <iostream>  
#include <limits> // 用于numeric_limits  
#include <libpmem.h>  // 添加持久内存库支持
#include <shared_mutex>
#include <mutex>
#include <algorithm>
#include <execution>
#include <future>
#include <vector>
#include <numeric>

using namespace std;
#define PMEM
#define PMEM_PATH "/mnt/pmem/MACuckoo/cuckoo_hash"  // 持久内存文件路径
#define PMEM_SIZE (1024 * 1024 * 1024) * 30ul  // 1GB 持久内存大小

#define KEY_TYPE uint64_t
#define VAL_TYPE uint64_t
#define SIG_TYPE uint16_t

#define KEY_LEN 8  
#define VAL_LEN 8  
#define MIRROR_RATE 2
#define BUCKET_SIZE 8
#define PMEM_BUCKET_SIZE MIRROR_RATE*BUCKET_SIZE  
#define TABLE1 0  
#define TABLE2 1  
#define HASH_SEED_FP 12345  
#define HASH_SEED_BUCKET 54321  
#define TEST false  
#define BITMAP_BYTESIZE 1
#define PMEM_BITMAP_BYTESIZE (BUCKET_SIZE*MIRROR_RATE)/8
#define THREAD_NUM 2
#define RW_LOCK_CK

#define getPmemtableID(x) ((((uint8_t)(x)) >> 7) & 1)
#define getPmemCellID(x) (((uint8_t)(x)) & (~(1<<7)))

#define LazyExpansion

using namespace std;  

//bitmap---
void bitmap_set(uint8_t bitmap[], size_t pos) {
    size_t byte_pos = pos / 8;
    uint8_t bit_pos = pos % 8;
    
    bitmap[byte_pos] |= (1 << bit_pos);
}

// 清除指定位 (设置为0)
void bitmap_clear(uint8_t bitmap[], size_t pos) {
    size_t byte_pos = pos / 8;
    uint8_t bit_pos = pos % 8;
    
    bitmap[byte_pos] &= ~(1 << bit_pos);
}

// 检查指定位是否为1
int bitmap_test(uint8_t bitmap[], size_t pos) {
    size_t byte_pos = pos / 8;
    uint8_t bit_pos = pos % 8;
    
    return (bitmap[byte_pos] & (1 << bit_pos)) ? 1 : 0;
}

int bitmap_find_first_zero(uint8_t bitmap[], size_t byteSize) {
    for (size_t i = 0; i < byteSize; i++) {
        uint8_t inverted = ~bitmap[i]; // 取反，使我们寻找1而不是0
        if (inverted) {
            // __builtin_ctz 计算尾随零的数量，即找到最低位的1
            return i * 8 + __builtin_ctz((unsigned int)inverted);
        }
    }
    return -1; // 所有位都是1
}

// 检查位图中是否存在值为0的位
// 返回true表示存在0位，false表示全为1
bool bitmap_has_zero(uint8_t bitmap[], size_t byteSize) {
    for (size_t i = 0; i < byteSize; i++) {
        if (bitmap[i] != 0xFF) {
            return true; // 找到一个不全为1的字节，说明存在0位
        }
    }
    return false; // 所有字节都是0xFF，位图中没有0位
}

int bitmap_count_ones(uint8_t bitmap[], size_t byteSize) {
    static const uint8_t BITS_SET_TABLE[256] = {
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
        4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
    };
    
    size_t count = 0;
    for (size_t i = 0; i < byteSize; i++) {
        count += BITS_SET_TABLE[bitmap[i]];
    }
    return count;
}
//--

struct Entry{  
    KEY_TYPE key;  
    VAL_TYPE val;  
};  

struct Bucket{  
    SIG_TYPE fp[BUCKET_SIZE];  
    uint8_t cell_cnt[BITMAP_BYTESIZE];
    uint8_t pmem_idx[BUCKET_SIZE];
};  

struct Table  
{  
    Bucket* bucket;   
};  

struct alignas(64) PmemBucket{  
    Entry pair[PMEM_BUCKET_SIZE];
};  

struct PmemTable  
{  
    uint8_t * expansion;
    PmemBucket* bucket;  
};  

int pmem_memory_use = 0;
int dram_memory_use = 0;

class CuckooHashTable {  
public:  
    Table table[2];  
    int max_kick_number;  
    int first_kick;
    int total_kick;
    int insert_cnt;
    // PmemHashTable pmem;  
    //---------------PMEM---------------//
    uint8_t *pmem_cell_cnt;
    PmemTable pmem_table[2];
    int bucket_number;
    void* pmem_addr;  // 持久内存映射地址
    size_t mapped_len; // 映射长度
    int is_pmem;      // 是否为真正的持久内存
    //-------------TIME-------------//
    std::chrono::duration<double> stage_1;
    std::chrono::duration<double> stage_2;
    std::chrono::duration<double> stage_3;
    //---------MUTEX-----------//
    #ifndef RW_LOCK_CK
    mutex *bucket_mut[2];   // mutex for bucket visition
    #endif

    #ifdef RW_LOCK_CK
    shared_mutex *bucket_mut[2];
    #endif
    void init_pmem(bool init_flag){
        size_t size = 2 * this->bucket_number * (sizeof(PmemBucket)+sizeof(uint8_t)*PMEM_BITMAP_BYTESIZE) + (2*bucket_number*PMEM_BUCKET_SIZE)/8;
        //size_t size = PMEM_SIZE;
        printf("pmem size: %lu , SIZE:%lu\n",size,PMEM_SIZE);
        // 创建持久内存文件目录（如果不存在）
        char path[256];
        sprintf(path, "%s_%d", PMEM_PATH, this->bucket_number);
        // 映射或创建持久内存文件
        if ((pmem_addr = pmem_map_file(path, PMEM_SIZE, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
            perror("pmem_map_file");
            exit(1);
        }
        printf("pemme addr : %p\n",pmem_addr);
        // 设置内存布局
        char* addr = (char*)pmem_addr;
        
        this->pmem_cell_cnt = (uint8_t*)addr;
        addr += (2*bucket_number*PMEM_BUCKET_SIZE)/8;
        // 设置TABLE1的bucket
        this->pmem_table[TABLE1].expansion = (uint8_t*)addr;
        addr += (this->bucket_number/8)*sizeof(uint8_t);
        this->pmem_table[TABLE1].bucket = (PmemBucket*)addr;
        addr += this->bucket_number * sizeof(PmemBucket);
        
        // 设置TABLE2的bucket
        this->pmem_table[TABLE2].expansion = (uint8_t*)addr;
        addr += (this->bucket_number/8)*sizeof(uint8_t);
        this->pmem_table[TABLE2].bucket = (PmemBucket*)addr;

        
                
        // 初始化内存（如果是新创建的文件）
            // 对于真正的持久内存，使用pmem_memset_persist
        if(is_pmem)
            printf("REAL PMEM\n");
        if(init_flag){
            pmem_memset_persist(pmem_cell_cnt,0,2*bucket_number*PMEM_BUCKET_SIZE/8);
            pmem_memset_persist(pmem_table[TABLE1].bucket, 0, this->bucket_number * sizeof(PmemBucket));
            pmem_memset_persist(pmem_table[TABLE2].bucket, 0, this->bucket_number * sizeof(PmemBucket));
            pmem_memset_persist(pmem_table[TABLE1].expansion, 0, (this->bucket_number/8 + 1)*sizeof(uint8_t));
            pmem_memset_persist(pmem_table[TABLE1].expansion, 0, (this->bucket_number/8 + 1)*sizeof(uint8_t));
        }
    }

    CuckooHashTable(int bucket_number, int max_kick_number,bool init_flag = true){  
        this->bucket_number = bucket_number;  
        this->max_kick_number = max_kick_number;  
        this->table[TABLE1].bucket = new Bucket[this->bucket_number];  
        memset(table[TABLE1].bucket,0,this->bucket_number*sizeof(Bucket));  
        this->table[TABLE2].bucket = new Bucket[this->bucket_number];  
        memset(table[TABLE2].bucket, 0, this->bucket_number*sizeof(Bucket));  
        #ifdef PMEM
        init_pmem(init_flag);
        #else
        this->pmem_table[TABLE1].bucket = new PmemBucket[this->bucket_number];  
        memset(pmem_table[TABLE1].bucket,0,this->bucket_number*sizeof(PmemBucket));  
        this->pmem_table[TABLE2].bucket = new PmemBucket[this->bucket_number];  
        memset(pmem_table[TABLE2].bucket, 0, this->bucket_number*sizeof(PmemBucket));  
        #endif

        #ifndef RW_LOCK_CK
		this->bucket_mut[TABLE1] = new mutex[this->bucket_number];
		this->bucket_mut[TABLE2] = new mutex[this->bucket_number];
        #endif
        #ifdef RW_LOCK_CK
        this->bucket_mut[TABLE1] = new shared_mutex[this->bucket_number];
		this->bucket_mut[TABLE2] = new shared_mutex[this->bucket_number];
        #endif

        this->first_kick = 0;
        this->total_kick = 0;
        this->insert_cnt = 0;
        dram_memory_use = (2*bucket_number*sizeof(Bucket)) + sizeof(CuckooHashTable);
    }  

    ~CuckooHashTable() {  
        delete [] table[TABLE1].bucket;  
        delete [] table[TABLE2].bucket;  
        #ifdef PMEM
        pmem_unmap(pmem_addr, mapped_len);
        #endif
        printf("Cuckoo Destory\n");
    }  

    void clear_stage(){
        stage_1 = std::chrono::duration<double>::zero();
        stage_2 = std::chrono::duration<double>::zero();
        stage_3 = std::chrono::duration<double>::zero();
    }

    bool inline bucket_lock(int table_idx, int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_CK
        if (THREAD_NUM > 1) {
            bucket_mut[table_idx][bucket_idx].lock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (THREAD_NUM > 1) {
            if (write_flag) bucket_mut[table_idx][bucket_idx].lock();
            else bucket_mut[table_idx][bucket_idx].lock_shared();
        }
        #endif
        return true;
    }

    bool inline bucket_unlock(int table_idx, int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_CK
        if (THREAD_NUM > 1) {
            bucket_mut[table_idx][bucket_idx].unlock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (THREAD_NUM > 1) {
            if (write_flag) bucket_mut[table_idx][bucket_idx].unlock();
            else bucket_mut[table_idx][bucket_idx].unlock_shared();
        }
        #endif
        return true;
    }

    void check_correct(){  
        for(int i=0;i<bucket_number;++i){  
            int counter_cnt = 0;  
            for(int j=0;j<BUCKET_SIZE;++j){  
                if(table[TABLE1].bucket[i].fp[j] != 0){  
                    counter_cnt += 1;  
                }  
            }  
            //检查oc count  
            int oc_c = 0;  
            for(int k=0;k<PMEM_BUCKET_SIZE;++k){  
                if(pmem_table[TABLE1].bucket[i].pair[k].key!=0){  
                    oc_c += 1;  
                }  
            }  
            if(oc_c != bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE)){  
                printf("oc count missmatch table1 idx:%d, real: %d, bitmap: %d\n",i,oc_c, bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE));  
            }  
            //检查cell数量是否正确  
            if(counter_cnt != bitmap_count_ones(table[TABLE1].bucket[i].cell_cnt,BITMAP_BYTESIZE)){  
                printf("cell miss match table1 idx:%d, real: %d, cell: %d\n",i,counter_cnt,bitmap_count_ones(table[TABLE1].bucket[i].cell_cnt,BITMAP_BYTESIZE));  
            }  
        }  

        for(int i=0;i<bucket_number;++i){  
            int counter_cnt = 0;  
            for(int j=0;j<BUCKET_SIZE;++j){  
                if(table[TABLE2].bucket[i].fp[j] != 0){  
                    counter_cnt += 1;  
                }  
            }  
            //检查oc count  
            int oc_c = 0;  
            for(int k=0;k<PMEM_BUCKET_SIZE;++k){  
                if(pmem_table[TABLE2].bucket[i].pair[k].key!=0){  
                    oc_c += 1;  
                }  
            }  
            if(oc_c != bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE)){  
                printf("bytesize:%d\n",PMEM_BITMAP_BYTESIZE);
                printf("oc count missmatch table2 idx:%d ,real: %d, bitmap: %d\n",i,oc_c,bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE));  
            }  
            //检查cell数量是否正确  
            if(counter_cnt != bitmap_count_ones(table[TABLE2].bucket[i].cell_cnt,BITMAP_BYTESIZE)){  
                printf("cell miss match table2 idx:%d, real: %d, cell: %d\n",i,counter_cnt,bitmap_count_ones(table[TABLE2].bucket[i].cell_cnt,BITMAP_BYTESIZE));  
            }  
        }  
        printf("--end checking--\n");  
    }  

    inline int getPmemBitmapIdx(int pmem_table, int pmem_bucket, int pmem_cell){
        return (pmem_table*bucket_number*PMEM_BUCKET_SIZE)+(pmem_bucket*PMEM_BUCKET_SIZE)+pmem_cell;
    }

    inline SIG_TYPE calculate_fp(KEY_TYPE key){  
        SIG_TYPE fp = MurmurHash3_x86_32(&key, KEY_LEN, HASH_SEED_FP);// % (1 << (SIG_LEN * 16));  
        
        return fp;  
    }  

    inline int calculate_hash_index(KEY_TYPE key){  
        return MurmurHash3_x86_32(&key,KEY_LEN,HASH_SEED_BUCKET) % this->bucket_number;  
    }  

    // inline int calculate_alternative_index(int index, char *fp){  
    //     uint32_t fingerprint;  
    //     fp_to_int(fingerprint,fp);  
    //     return index ^ (fingerprint % this->bucket_number);  
    // }  

    inline int hash_alt(int h_now, int table_now, SIG_TYPE fp) {
        if(table_now == TABLE1)
            return (h_now + (int(fp % bucket_number))) % bucket_number;
        else if(table_now == TABLE2) {
            int h_alt = (h_now - (int(fp % bucket_number))) % bucket_number;
            if(h_alt < 0) h_alt += bucket_number;
            return h_alt;
        }
        return 0;
    }

    inline int kick(int table_idx, int bucket_index, int alt_index, int cell_index){  
        int cur_index = bucket_index;  
        int cur_cell_index = cell_index;
        SIG_TYPE cur_fp;   
        int kick_times = 0;  
        int cur_table = table_idx;  
        bool find_empty = false;  

        stack<int> k_table;
        stack<int> k_bucket;
        stack<int> k_cell;
        k_table.push(table_idx); k_bucket.push(bucket_index); k_cell.push(cell_index);
        std::unordered_set<int> S1, S2;  
        while(find_empty == false && kick_times < this->max_kick_number){  
            //printf("times:%d,cur bucket idx:%d, cur cell idx:%d\n",kick_times,cur_index,cur_cell_index);
            cur_fp = table[cur_table].bucket[cur_index].fp[cur_cell_index];  
            // printf("kick index : %d table: %d kick src: %d ",kick_index,TABLE1,cur_index);  
            
            //cur_index = calculate_alternative_index(cur_index,cur_fp);  
            cur_index = hash_alt(cur_index,cur_table,cur_fp);
            cur_table = 1-cur_table;  
            // printf(", kick dst: %d\n",cur_index);
            if((cur_table!=table_idx||cur_index!=bucket_index) && (cur_table!=1-table_idx||cur_index!=alt_index))  
                bucket_lock(cur_table,cur_index,true);
            int empty_index = bitmap_find_first_zero(table[cur_table].bucket[cur_index].cell_cnt,BITMAP_BYTESIZE);
            if(empty_index!=-1){  
                bitmap_set(table[cur_table].bucket[cur_index].cell_cnt,empty_index);
                find_empty = true;
                cur_cell_index = empty_index;
            }else{
                cur_cell_index = cell_index;
            }  
            if((cur_table!=table_idx||cur_index!=bucket_index) && (cur_table!=1-table_idx||cur_index!=alt_index))  
                bucket_unlock(cur_table,cur_index,true);
            k_table.push(cur_table); k_bucket.push(cur_index); k_cell.push(cur_cell_index);
           
            kick_times += 1;  
            if(kick_times>0&&cur_table==table_idx&&cur_index==bucket_index){return -1;}
        }  

        if(find_empty == true){  
            total_kick += 1;
            int switch_table_idx = k_table.top();  
            int switch_bucket_idx = k_bucket.top(); 
            int switch_cell_idx = k_cell.top(); 
            k_table.pop();
            k_bucket.pop();
            k_cell.pop();
            while(!k_bucket.empty()){  
                int t = k_table.top();
                int b = k_bucket.top();
                int c = k_cell.top();
                SIG_TYPE temp_fp;
                uint8_t temp_pmem_idx1, temp_pmem_idx2;
                if((t!=table_idx||b!=bucket_index) && (t!=1-table_idx||b!=alt_index))  
                    bucket_lock(t,b,false);
                temp_fp = table[t].bucket[b].fp[c];
                temp_pmem_idx1 = table[t].bucket[b].pmem_idx[c];
                if((t!=table_idx||b!=bucket_index) && (t!=1-table_idx||b!=alt_index))  
                    bucket_unlock(t,b,false);
                
                if((switch_table_idx!=table_idx||switch_bucket_idx!=bucket_index) && (switch_table_idx!=1-table_idx||switch_bucket_idx!=alt_index))  
                    bucket_lock(switch_table_idx,switch_bucket_idx,true);
                table[switch_table_idx].bucket[switch_bucket_idx].fp[switch_cell_idx] = temp_fp;
                table[switch_table_idx].bucket[switch_bucket_idx].pmem_idx[switch_cell_idx] = temp_pmem_idx1;
                if((switch_table_idx!=table_idx||switch_bucket_idx!=bucket_index) && (switch_table_idx!=1-table_idx||switch_bucket_idx!=alt_index))  
                    bucket_unlock(switch_table_idx,switch_bucket_idx,true);
                switch_table_idx = t;
                switch_bucket_idx = b;
                switch_cell_idx = c;

                k_table.pop();
                k_bucket.pop();
                k_cell.pop();
            }
            if(switch_table_idx!=table_idx||switch_bucket_idx != bucket_index||switch_cell_idx!=cell_index){
                printf("STH WRONG!!!!!!!!\n");
            }
            bitmap_clear(table[switch_table_idx].bucket[switch_bucket_idx].cell_cnt,switch_cell_idx);
            return 1;  
        }  
        //printf("reach kick lim\n");  
        return 0;  
    }  

    inline bool insert(Entry& entry){  
        SIG_TYPE fp_i = calculate_fp(entry.key);  
        int idx1 = calculate_hash_index(entry.key);  
        //int idx2 = calculate_alternative_index(idx1,fp_c);  
        int idx2 = hash_alt(idx1,TABLE1,fp_i);
        bucket_lock(TABLE1,idx1,true);
        bucket_lock(TABLE2,idx2,true);
        //lazy_expansion_check(TABLE1,idx1);
        //lazy_expansion_check(TABLE2,idx2);

        for(int i=0;i<BUCKET_SIZE;i++){  
            if((bitmap_test(table[TABLE1].bucket[idx1].cell_cnt,i) == 1 && fp_i == table[TABLE1].bucket[idx1].fp[i])){  
                int pmem_table_idx = getPmemtableID(table[TABLE1].bucket[idx1].pmem_idx[i]);
                int pmem_bucket_idx = pmem_table==TABLE1?idx1:idx2;
                int pmem_cell_idx = getPmemCellID(table[TABLE1].bucket[idx1].pmem_idx[i]);
                if(pmem_table[pmem_table_idx].bucket[pmem_bucket_idx].pair[pmem_cell_idx].key == entry.key){
                    printf("Duplicate key\n");
                    bucket_unlock(TABLE1,idx1,true);
                    bucket_unlock(TABLE2,idx2,true); 
                    return true;
                }
            }  
            if(bitmap_test(table[TABLE2].bucket[idx2].cell_cnt,i) == 1 && fp_i == table[TABLE2].bucket[idx2].fp[i]){
                int pmem_table_idx = getPmemtableID(table[TABLE2].bucket[idx2].pmem_idx[i]);
                int pmem_bucket_idx = pmem_table==TABLE1?idx1:idx2;
                int pmem_cell_idx = getPmemCellID(table[TABLE2].bucket[idx2].pmem_idx[i]);
                if(pmem_table[pmem_table_idx].bucket[pmem_bucket_idx].pair[pmem_cell_idx].key == entry.key){
                    printf("Duplicate key\n");
                    bucket_unlock(TABLE1,idx1,true);
                    bucket_unlock(TABLE2,idx2,true); 
                    return true;
                }
            }
        }  

        int bucket_size1 = bitmap_count_ones(table[TABLE1].bucket[idx1].cell_cnt,BITMAP_BYTESIZE);
        int bucket_size2 = bitmap_count_ones(table[TABLE2].bucket[idx2].cell_cnt,BITMAP_BYTESIZE);
        int pmem_bucket_size1 = bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+idx1*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);
        int pmem_bucket_size2 = bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number*PMEM_BITMAP_BYTESIZE+idx2*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);
        // auto start = std::chrono::high_resolution_clock::now();

        // auto end = std::chrono::high_resolution_clock::now();
        // stage_1 += end-start;
        bool insert_flag = false;
        int insert_cell = -1, insert_table = -1, insert_bucket = -1;
        int pmem_insert_cell = -1, pmem_insert_table = -1, pmem_insert_bucket = -1;
///

        if(pmem_bucket_size1 == PMEM_BUCKET_SIZE && pmem_bucket_size2 == PMEM_BUCKET_SIZE){
            bucket_unlock(TABLE1,idx1,true);
            bucket_unlock(TABLE2,idx2,true); 
            printf("OC NO SPACE key: %lu, fp:%d, idx1:%d, idx2:%d\n",entry.key,fp_i,idx1,idx2);  
            // printf("cell count od idx1 & idx2: %d||%d, oc cell count of idx1 & idx2 : %d || %d\n",table[TABLE1].cell[idx1],table[TABLE2].cell[idx2],
            // bitmap_count_ones(table[TABLE1].bucket[idx1].pmem_cell_cnt,PMEM_BITMAP_BYTESIZE),bitmap_count_ones(table[TABLE2].bucket[idx2].pmem_cell_cnt,PMEM_BITMAP_BYTESIZE));
            return false;  
        }

        pmem_insert_table = pmem_bucket_size1<pmem_bucket_size2?TABLE1:TABLE2;
        pmem_insert_bucket = pmem_bucket_size1<pmem_bucket_size2?idx1:idx2;
        pmem_insert_cell = bitmap_find_first_zero(&pmem_cell_cnt[pmem_insert_table*bucket_number*PMEM_BITMAP_BYTESIZE+pmem_insert_bucket*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);

        if(bucket_size1<BUCKET_SIZE || bucket_size2<BUCKET_SIZE){
            insert_flag = true;
            //均衡
            insert_table = bucket_size1<bucket_size2?TABLE1:TABLE2;
            insert_bucket = bucket_size1<bucket_size2?idx1:idx2;
            insert_cell = bitmap_find_first_zero(table[insert_table].bucket[insert_bucket].cell_cnt,BITMAP_BYTESIZE);
        }

        // start = std::chrono::high_resolution_clock::now();
        if(insert_flag==false){
            int first_kick_table = pmem_bucket_size1<pmem_bucket_size2?TABLE1:TABLE2;
            int idx = first_kick_table==TABLE1?idx1:idx2;
            int alt_idx = first_kick_table==TABLE1?idx2:idx1;
            for(int i=0;i<BUCKET_SIZE;i++){
                if(kick(first_kick_table,idx,alt_idx,i)==1){
                    insert_flag = true;
                    insert_table = first_kick_table;
                    insert_bucket = first_kick_table == TABLE1?idx1:idx2;
                    insert_cell = i;
                    break;
                }
                if(kick(1-first_kick_table,alt_idx,idx,i)==1){
                    insert_flag = true;
                    insert_table = 1-first_kick_table;
                    insert_bucket = (1-first_kick_table) == TABLE1?idx1:idx2;
                    insert_cell = i;
                    break;
                }
            }
        }

        if(insert_flag == false || insert_table == -1){
            bucket_unlock(TABLE1,idx1,true);
            bucket_unlock(TABLE2,idx2,true);
            printf("KICK FAIL key: %lu, fp:%d, idx1:%d, idx2:%d\n",entry.key,fp_i,idx1,idx2);  
            return false;  
        }
        // end = std::chrono::high_resolution_clock::now();
        // stage_2 += end-start;

        // start = std::chrono::high_resolution_clock::now();
        //-------------
        table[insert_table].bucket[insert_bucket].fp[insert_cell] = fp_i;  
        bitmap_set(table[insert_table].bucket[insert_bucket].cell_cnt,insert_cell);  
        table[insert_table].bucket[insert_bucket].pmem_idx[insert_cell] = (((uint8_t)pmem_insert_table<<7)&(1<<7))|((uint8_t)pmem_insert_cell & (~(1<<7)));
        bitmap_set(pmem_cell_cnt,getPmemBitmapIdx(pmem_insert_table,pmem_insert_bucket,pmem_insert_cell));

        __builtin_prefetch(&pmem_table[pmem_insert_table].bucket[pmem_insert_bucket], 1, 2);
        #ifdef PMEM
        pmem_memcpy_nodrain(&(pmem_table[pmem_insert_table].bucket[pmem_insert_bucket].pair[pmem_insert_cell]), &entry,sizeof(Entry)); 
        if(insert_cnt%999999==0)
            pmem_drain();
        #else
        pmem_table[pmem_insert_table].bucket[pmem_insert_bucket].pair[pmem_insert_cell] = entry;
        #endif
        // end = std::chrono::high_resolution_clock::now();
        // stage_3 += (end-start);

        //-------------
        bucket_unlock(TABLE1,idx1,true);
        bucket_unlock(TABLE2,idx2,true);
        return true;  
    }  

    inline bool query(KEY_TYPE InKey, char *val){
        // auto end = std::chrono::high_resolution_clock::now();
        // stage_1 += end-start;

        // start = std::chrono::high_resolution_clock::now();
        SIG_TYPE fp_i = calculate_fp(InKey);  
        int idx1 = calculate_hash_index(InKey);  
        // int idx2 = calculate_alternative_index(idx1,fp_c);  
        int idx2 = hash_alt(idx1,TABLE1,fp_i);

        bucket_lock(TABLE1,idx1,false);
        bucket_lock(TABLE2,idx2,false);
        __builtin_prefetch(&pmem_table[TABLE2].bucket[idx2], 0, 2);
        __builtin_prefetch(&pmem_table[TABLE1].bucket[idx1], 0, 2);
        for(int i=0;i<BUCKET_SIZE;++i){  
            if(table[TABLE2].bucket[idx2].fp[i] == fp_i){  
                int pmem_table_idx = getPmemtableID(table[TABLE2].bucket[idx2].pmem_idx[i]);
                int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
                int pmem_cell = getPmemCellID(table[TABLE2].bucket[idx2].pmem_idx[i]);

                if(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == InKey) {
                    memcpy(val, &(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].val), VAL_LEN);
                    bucket_unlock(TABLE1,idx1,false);
                    bucket_unlock(TABLE2,idx2,false);
                    return true;
                } 
            }  
        }  
        for(int i=0;i<BUCKET_SIZE;++i){  
            if(table[TABLE1].bucket[idx1].fp[i] == fp_i){  
                int pmem_table_idx = getPmemtableID(table[TABLE1].bucket[idx1].pmem_idx[i]);
                int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
                int pmem_cell = getPmemCellID(table[TABLE1].bucket[idx1].pmem_idx[i]);

                if(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == InKey) {
                    memcpy(val, &(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].val), VAL_LEN);
                    bucket_unlock(TABLE1,idx1,false);
                    bucket_unlock(TABLE2,idx2,false);
                    return true;
                } 
            }  
        }
        bucket_unlock(TABLE1,idx1,false);
        bucket_unlock(TABLE2,idx2,false);
        // end = std::chrono::high_resolution_clock::now();
        // stage_3 += end-start;
        return false;  
    }  

    inline bool deletion(KEY_TYPE key){  
        SIG_TYPE fp_i = calculate_fp(key);   
        int idx1 = calculate_hash_index(key);  
        // int idx2 = calculate_atlternative_index(idx1,fp_c);  
        int idx2 = hash_alt(idx1,TABLE1,fp_i);
        bucket_lock(TABLE1,idx1,true);
        bucket_lock(TABLE2,idx2,true);
        __builtin_prefetch(&pmem_table[TABLE1].bucket[idx1], 0, 2);
        __builtin_prefetch(&pmem_table[TABLE2].bucket[idx2], 0, 2);

        for(int i=0;i<BUCKET_SIZE;++i){  
            int pmem_table_idx = getPmemtableID(table[TABLE2].bucket[idx2].pmem_idx[i]);
            int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
            int pmem_cell = getPmemCellID(table[TABLE2].bucket[idx2].pmem_idx[i]);

            if(bitmap_test(table[TABLE2].bucket[idx2].cell_cnt,i) == 1 && table[TABLE2].bucket[idx2].fp[i] == fp_i
             && pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == key){  
                bitmap_clear(table[TABLE2].bucket[idx2].cell_cnt,i);
                bitmap_clear(pmem_cell_cnt, getPmemBitmapIdx(pmem_table_idx,pmem_bucket,pmem_cell));

                bucket_unlock(TABLE1,idx1,true);
                bucket_unlock(TABLE2,idx2,true);
                return true;
            } 
        }  
        for(int i=0;i<BUCKET_SIZE;++i){  
            int pmem_table_idx = getPmemtableID(table[TABLE1].bucket[idx1].pmem_idx[i]);
            int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
            int pmem_cell = getPmemCellID(table[TABLE1].bucket[idx1].pmem_idx[i]);

            if(bitmap_test(table[TABLE1].bucket[idx1].cell_cnt,i) == 1 && table[TABLE1].bucket[idx1].fp[i] == fp_i 
            && pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == key){  
                bitmap_clear(table[TABLE1].bucket[idx1].cell_cnt,i);
                bitmap_clear(pmem_cell_cnt, getPmemBitmapIdx(pmem_table_idx,pmem_bucket,pmem_cell));
                
                bucket_unlock(TABLE1,idx1,true);
                bucket_unlock(TABLE2,idx2,true);
                return true;
            }  
        }  
        bucket_unlock(TABLE1,idx1,true);
        bucket_unlock(TABLE2,idx2,true); 
        return false;  
    }  

    inline bool update(Entry entry){  
        SIG_TYPE fp_i = calculate_fp(entry.key);  
        int idx1 = calculate_hash_index(entry.key);  
        //int idx2 = calculate_alternative_index(idx1,fp_c);  
        int idx2 = hash_alt(idx1,TABLE1,fp_i);

        bucket_lock(TABLE1,idx1,true);
        bucket_lock(TABLE2,idx2,true);
        __builtin_prefetch(&pmem_table[TABLE1].bucket[idx1], 0, 2);
        __builtin_prefetch(&pmem_table[TABLE2].bucket[idx2], 0, 2);
        for(int i=0;i<BUCKET_SIZE;++i){  
            if(bitmap_test(table[TABLE2].bucket[idx2].cell_cnt,i) == 1 && table[TABLE2].bucket[idx2].fp[i] == fp_i ){  
                int pmem_table_idx = getPmemtableID(table[TABLE2].bucket[idx2].pmem_idx[i]);
                int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
                int pmem_cell = getPmemCellID(table[TABLE2].bucket[idx2].pmem_idx[i]);
                if(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == entry.key){
                    #ifdef PMEM
                    pmem_memcpy_nodrain(&(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell]),&entry,sizeof(entry));
                    pmem_drain();
                    #else
                    pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].val = entry.val;
                    #endif
                    bucket_unlock(TABLE1,idx1,true);
                    bucket_unlock(TABLE2,idx2,true);
                    return true;
                }
            } 
            if(bitmap_test(table[TABLE1].bucket[idx1].cell_cnt,i) == 1 && table[TABLE1].bucket[idx1].fp[i] == fp_i){  
                int pmem_table_idx = getPmemtableID(table[TABLE1].bucket[idx1].pmem_idx[i]);
                int pmem_bucket = (pmem_table_idx == TABLE1)?idx1:idx2;
                int pmem_cell = getPmemCellID(table[TABLE1].bucket[idx1].pmem_idx[i]);
                if(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].key == entry.key){
                    #ifdef PMEM
                    pmem_memcpy_nodrain(&(pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell]),&entry,sizeof(entry));
                    pmem_drain();
                    #else
                    pmem_table[pmem_table_idx].bucket[pmem_bucket].pair[pmem_cell].val = entry.val;
                    #endif
                    bucket_unlock(TABLE1,idx1,true);
                    bucket_unlock(TABLE2,idx2,true);
                    return true;
                }
            }  
        }  

        bucket_unlock(TABLE1,idx1,true);
        bucket_unlock(TABLE2,idx2,true); 
 
        return false;
    }  

    inline bool recover_insert(Entry& entry, int pmem_table, int pmem_bucket, int pmem_cell){  
        SIG_TYPE fp_i = calculate_fp(entry.key);  
        int idx1 = calculate_hash_index(entry.key);  
        //int idx2 = calculate_alternative_index(idx1,fp_c);  
        int idx2 = hash_alt(idx1,TABLE1,fp_i);
        // if(id <15000000) printf("AAA-GETLOCK2---%d\n",id);

        int bucket_size1 = bitmap_count_ones(table[TABLE1].bucket[idx1].cell_cnt,BITMAP_BYTESIZE);
        int bucket_size2 = bitmap_count_ones(table[TABLE2].bucket[idx2].cell_cnt,BITMAP_BYTESIZE);
       
        // auto start = std::chrono::high_resolution_clock::now();

        // auto end = std::chrono::high_resolution_clock::now();
        // stage_1 += end-start;
        bool insert_flag = false;
        int insert_cell = -1, insert_table = -1, insert_bucket = -1;

        if(bucket_size1<BUCKET_SIZE || bucket_size2<BUCKET_SIZE){
            insert_flag = true;
            //均衡
            insert_table = bucket_size1<bucket_size2?TABLE1:TABLE2;
            insert_bucket = bucket_size1<bucket_size2?idx1:idx2;
            insert_cell = bitmap_find_first_zero(table[insert_table].bucket[insert_bucket].cell_cnt,BITMAP_BYTESIZE);
        }

        // start = std::chrono::high_resolution_clock::now();
        if(insert_flag==false){
            int first_kick_table = pmem_table;
            int idx = first_kick_table==TABLE1?idx1:idx2;
            int alt_idx = first_kick_table==TABLE1?idx2:idx1;
            for(int i=0;i<BUCKET_SIZE;i++){
                int kick_res1 = kick(first_kick_table,idx,alt_idx,i);
                if(kick_res1==1){
                    insert_flag = true;
                    insert_table = first_kick_table;
                    insert_bucket = first_kick_table == TABLE1?idx1:idx2;
                    insert_cell = i;
                    break;
                }
                int kick_res2 = kick(1-first_kick_table,alt_idx,idx,i);
                if(kick_res2==1){
                    insert_flag = true;
                    insert_table = 1-first_kick_table;
                    insert_bucket = (1-first_kick_table) == TABLE1?idx1:idx2;
                    insert_cell = i;
                    break;
                }
            }
        }

        if(insert_flag == false || insert_table == -1){
            printf("RECOVERY FAIL key: %lu, fp:%d, idx1:%d, idx2:%d, pmem_t,b,c:%d,%d,%d\n",entry.key,fp_i,idx1,idx2,pmem_table,pmem_bucket,pmem_cell);  
            return false;  
        }
        // end = std::chrono::high_resolution_clock::now();
        // stage_2 += end-start;

        // start = std::chrono::high_resolution_clock::now();
        //-------------
        table[insert_table].bucket[insert_bucket].fp[insert_cell] = fp_i;  
        bitmap_set(table[insert_table].bucket[insert_bucket].cell_cnt,insert_cell);  
        table[insert_table].bucket[insert_bucket].pmem_idx[insert_cell] = (((uint8_t)pmem_table<<7)&(1<<7))|((uint8_t)pmem_cell & (~(1<<7)));

        // end = std::chrono::high_resolution_clock::now();
        // stage_3 += (end-start);

        //-------------
        bucket_unlock(TABLE1,idx1,true);
        bucket_unlock(TABLE2,idx2,true);
        return true;  
    }  
    bool recover(){
        cout<<"start recover"<<endl;
        for(int i=0;i<PMEM_BUCKET_SIZE;++i){
            bool expansion_flag1=false, expansion_flag2=false;
            // if(bitmap_test(pmem_table[TABLE1].expansion,i)){
            //     // if(lazy_expansion_check(TABLE1,i));
            // }    
            // if(bitmap_test(pmem_table[TABLE2].expansion,i)){
            //     //lazy_expansion_check(TABLE2,i);
            // }    
            for(int j=0;j<bucket_number;++j){
                if(bitmap_test(pmem_cell_cnt,getPmemBitmapIdx(TABLE1,j,i))){
                    if(!recover_insert(pmem_table[TABLE1].bucket[j].pair[i],TABLE1,j,i)){
                        //expansion and reinsert
                        return false;
                    }
                }
            }
            for(int j=0;j<bucket_number;++j){
                if(bitmap_test(pmem_cell_cnt,getPmemBitmapIdx(TABLE2,j,i))){
                    if(!recover_insert(pmem_table[TABLE2].bucket[j].pair[i],TABLE2,j,i)){
                        //expansion and reinsert
                        return false;
                    }
                }
            }
        }

        return true;
    }

    void expansion(int expansion_rate=2){
        for(int i=0;i<(bucket_number/8);++i){
            pmem_table[TABLE1].expansion[i] |= ~(uint8_t)0;
            pmem_table[TABLE2].expansion[i] |= ~(uint8_t)0;
        }
        int new_bucket_number = expansion_rate * bucket_number;
        size_t new_pmem_size = 2 * new_bucket_number* (sizeof(PmemBucket)+sizeof(uint8_t)*PMEM_BITMAP_BYTESIZE) + (2*new_bucket_number*PMEM_BUCKET_SIZE)/8;
        size_t new_mapped_len;
        void* new_pmem_addr;
        char path[256];
        sprintf(path, "%s_%d", PMEM_PATH, new_bucket_number);
        // 映射或创建持久内存文件
        if ((new_pmem_addr = pmem_map_file(path, new_pmem_size, PMEM_FILE_CREATE, 0666, &new_mapped_len, &is_pmem)) == NULL) {
            perror("pmem_map_file");
            exit(1);
        }
        char* addr = (char*)new_pmem_addr;
        //printf("new pmem addr:%p\n",addr);
        uint8_t* new_pmem_cell_cnt = (uint8_t*)addr;
        addr += ((2*new_bucket_number*PMEM_BUCKET_SIZE)/8);
        for(int i=0;i<expansion_rate;++i){
            memcpy(new_pmem_cell_cnt+i*((2*bucket_number*PMEM_BUCKET_SIZE)/8),pmem_cell_cnt,(2*bucket_number*PMEM_BUCKET_SIZE)/8);
        }

        pmem_cell_cnt = new_pmem_cell_cnt;
        uint8_t* expansion = (uint8_t*)addr;
        addr += (new_bucket_number/8);
        for(int i=0;i<expansion_rate;++i){
            memcpy(expansion+i*(bucket_number/8),pmem_table[TABLE1].expansion,bucket_number/8);
        }

        pmem_table[TABLE1].expansion = expansion;
        PmemBucket* bucket = (PmemBucket*)addr;
        addr += (new_bucket_number*sizeof(PmemBucket));
        for(int i=0;i<expansion_rate;++i){
            memcpy(bucket+i*(bucket_number),pmem_table[TABLE1].bucket,bucket_number*sizeof(PmemBucket));
        }
        pmem_table[TABLE1].bucket = bucket;
        expansion = (uint8_t*)addr;
        addr += (new_bucket_number/8);
        for(int i=0;i<expansion_rate;++i){
            memcpy(expansion+i*(bucket_number/8),pmem_table[TABLE2].expansion,bucket_number/8);
        }
        pmem_table[TABLE2].expansion = expansion;
        bucket = (PmemBucket*)addr;
        addr += (new_bucket_number*sizeof(PmemBucket));
        for(int i=0;i<expansion_rate;++i){
            memcpy(bucket+i*(bucket_number),pmem_table[TABLE2].bucket,bucket_number*sizeof(PmemBucket));
        }
        pmem_table[TABLE2].bucket = bucket;

        Bucket* new_bucket = (Bucket*) malloc(new_bucket_number*sizeof(Bucket));
        for(int i=0;i<expansion_rate;++i){
            memcpy(new_bucket + bucket_number * i,table[TABLE1].bucket,bucket_number*sizeof(Bucket));
        }
        table[TABLE1].bucket = new_bucket;
        new_bucket = (Bucket*) malloc(new_bucket_number*sizeof(Bucket));
        for(int i=0;i<expansion_rate;++i){
            memcpy(new_bucket + bucket_number * i,table[TABLE2].bucket,bucket_number*sizeof(Bucket));
        }
        table[TABLE2].bucket = new_bucket;

        this->bucket_number = new_bucket_number;
        pmem_unmap(pmem_addr, mapped_len);
        pmem_addr = new_pmem_addr;
        mapped_len = new_mapped_len;
        //pmem_persist(pmem_addr,new_mapped_len);

        //test lazy check
        #ifndef LazyExpansion
        //cal_load_factor();
        for(int i=0;i<this->bucket_number;++i){
            lazy_expansion_check(TABLE1,i);
            lazy_expansion_check(TABLE2,i);
        }
        //cal_load_factor();
        #endif
    }


    void expansion_par(int expansion_rate = 2) {
        // 1. 并行设置 expansion 位图 (仅针对 table1 和 table2)
        auto set_expansion = [&](int table_id) {
            for(int i = 0; i < (bucket_number/8); ++i) {
                pmem_table[table_id].expansion[i] |= ~(uint8_t)0;
            }
        };
        
        // 异步执行两个表的设置
        auto future1 = std::async(std::launch::async, set_expansion, TABLE1);
        auto future2 = std::async(std::launch::async, set_expansion, TABLE2);
        future1.wait();
        future2.wait();

        // 2. 计算新的内存布局 (顺序执行)
        int new_bucket_number = expansion_rate * bucket_number;
        size_t new_pmem_size = 2 * new_bucket_number * (sizeof(PmemBucket) + sizeof(uint8_t) * PMEM_BITMAP_BYTESIZE) + 
                            (2 * new_bucket_number * PMEM_BUCKET_SIZE) / 8;
        size_t new_mapped_len;
        void* new_pmem_addr;
        char path[256];
        sprintf(path, "%s_%d", PMEM_PATH, new_bucket_number);
        
        // 映射或创建持久内存文件
        if ((new_pmem_addr = pmem_map_file(path, new_pmem_size, PMEM_FILE_CREATE, 0666, &new_mapped_len, &is_pmem)) == NULL) {
            perror("pmem_map_file");
            exit(1);
        }

        char* addr = (char*)new_pmem_addr;
        
        // 3. 顺序复制 pmem_cell_cnt
        uint8_t* new_pmem_cell_cnt = (uint8_t*)addr;
        addr += ((2 * new_bucket_number * PMEM_BUCKET_SIZE) / 8);
        
        const size_t cell_cnt_size = (2 * bucket_number * PMEM_BUCKET_SIZE) / 8;
        for(int i = 0; i < expansion_rate; ++i) {
            memcpy(new_pmem_cell_cnt + i * cell_cnt_size, pmem_cell_cnt, cell_cnt_size);
        }

        pmem_cell_cnt = new_pmem_cell_cnt;

        // 4. 并行处理 TABLE1 和 TABLE2 数据
        auto process_table = [&](int table_id, char* start_addr) -> std::tuple<uint8_t*, PmemBucket*> {
            char* addr_ref = start_addr;
            
            // expansion 数据
            uint8_t* expansion = (uint8_t*)addr_ref;
            addr_ref += (new_bucket_number / 8);
            
            const size_t expansion_size = bucket_number / 8;
            for(int i = 0; i < expansion_rate; ++i) {
                memcpy(expansion + i * expansion_size, 
                    pmem_table[table_id].expansion, 
                    expansion_size);
            }
            
            // bucket 数据
            PmemBucket* bucket = (PmemBucket*)addr_ref;
            addr_ref += (new_bucket_number * sizeof(PmemBucket));
            
            const size_t bucket_size = bucket_number * sizeof(PmemBucket);
            for(int i = 0; i < expansion_rate; ++i) {
                memcpy(bucket + i * bucket_number, 
                    pmem_table[table_id].bucket, 
                    bucket_size);
            }
            
            return std::make_tuple(expansion, bucket);
        };

        // 异步处理两个表
        auto table1_future = std::async(std::launch::async, [&]() {
            return process_table(TABLE1, addr);
        });
        
        // 计算 TABLE2 的起始地址
        char* table2_addr = addr + (new_bucket_number / 8) + (new_bucket_number * sizeof(PmemBucket));
        auto table2_future = std::async(std::launch::async, [&]() {
            return process_table(TABLE2, table2_addr);
        });

        // 等待完成并更新指针
        auto table1_result = table1_future.get();
        auto table2_result = table2_future.get();
        
        pmem_table[TABLE1].expansion = std::get<0>(table1_result);
        pmem_table[TABLE1].bucket = std::get<1>(table1_result);
        pmem_table[TABLE2].expansion = std::get<0>(table2_result);
        pmem_table[TABLE2].bucket = std::get<1>(table2_result);

        // 5. 并行处理内存表 (table1 和 table2)
        auto process_memory_table = [&](int table_id) -> Bucket* {
            Bucket* new_bucket = (Bucket*)malloc(new_bucket_number * sizeof(Bucket));
            const size_t bucket_size = bucket_number * sizeof(Bucket);
            
            for(int i = 0; i < expansion_rate; ++i) {
                memcpy(new_bucket + bucket_number * i, 
                    table[table_id].bucket, 
                    bucket_size);
            }
            
            return new_bucket;
        };

        // 异步处理两个内存表
        auto mem_table1_future = std::async(std::launch::async, process_memory_table, TABLE1);
        auto mem_table2_future = std::async(std::launch::async, process_memory_table, TABLE2);
        
        table[TABLE1].bucket = mem_table1_future.get();
        table[TABLE2].bucket = mem_table2_future.get();

        // 6. 更新全局状态 (顺序执行)
        this->bucket_number = new_bucket_number;
        pmem_unmap(pmem_addr, mapped_len);
        pmem_addr = new_pmem_addr;
        mapped_len = new_mapped_len;

        // 7. 顺序执行 lazy expansion check
        #ifndef LazyExpansion
        for(int i = 0; i < this->bucket_number; ++i) {
            lazy_expansion_check(TABLE1, i);
            lazy_expansion_check(TABLE2, i);
        }
        #endif
    }


    void lazy_expansion_check(int tableId, int bucketId){
        if(bitmap_test(pmem_table[tableId].expansion,bucketId)!=1)
            return;
        
        for(int i=0;i<PMEM_BUCKET_SIZE;++i){
            KEY_TYPE key = pmem_table[tableId].bucket[bucketId].pair[i].key;
            SIG_TYPE fp_i = calculate_fp(key);  
            int idx1 = calculate_hash_index(key);  
            int idx2 = hash_alt(idx1,TABLE1,fp_i);
            int cur_idx = tableId==TABLE1?idx1:idx2;
            if(cur_idx != bucketId){
                bitmap_clear(pmem_cell_cnt,getPmemBitmapIdx(tableId,bucketId,i));
            }
        }
        
        for(int i=0;i<BUCKET_SIZE;++i){
            int alt_bucketId = hash_alt(bucketId,tableId,table[tableId].bucket[bucketId].fp[i]);
            int key_tableId = getPmemtableID(table[tableId].bucket[bucketId].pmem_idx[i]);

            int key_bucketId = key_tableId==tableId?bucketId:alt_bucketId;
            int key_cellId = getPmemCellID(table[tableId].bucket[bucketId].pmem_idx[i]);
            KEY_TYPE key = pmem_table[key_tableId].bucket[key_bucketId].pair[key_cellId].key;
            
            SIG_TYPE fp_i = table[tableId].bucket[bucketId].fp[i];
            int idx1 = calculate_hash_index(key);  
            int idx2 = hash_alt(idx1,TABLE1,fp_i);
            int cur_idx = tableId==TABLE1?idx1:idx2;
            if(cur_idx != bucketId){
                bitmap_clear(table[tableId].bucket[bucketId].cell_cnt,i);
            }
        }
    }

    int cal_load_factor(){  
        int cnt1 = 0, cnt2 = 0;  
        int pmem_cnt1 = 0, pmem_cnt2 = 0;
        int max_pmem_cnt = 0;
        for(int i=0;i<bucket_number;++i){  
            cnt1 += bitmap_count_ones(table[TABLE1].bucket[i].cell_cnt,BITMAP_BYTESIZE);  
            cnt2 += bitmap_count_ones(table[TABLE2].bucket[i].cell_cnt,BITMAP_BYTESIZE);    
            pmem_cnt1 += bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);
            pmem_cnt2 += bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE); 
            if(bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE) > max_pmem_cnt){
                max_pmem_cnt = bitmap_count_ones(&pmem_cell_cnt[TABLE1*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);
            }
            if(bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE)>max_pmem_cnt){
                max_pmem_cnt = bitmap_count_ones(&pmem_cell_cnt[TABLE2*bucket_number*PMEM_BITMAP_BYTESIZE+i*PMEM_BITMAP_BYTESIZE],PMEM_BITMAP_BYTESIZE);
            }
        }  
        printf("CELL cnt cell1:%d, cell2:%d, pm1: %d, pm2: %d, max_pmem_cnt: %d\n",cnt1,cnt2,pmem_cnt1,pmem_cnt2,max_pmem_cnt);
        printf("total kick:%d\n",total_kick);
        int cnt = cnt1+cnt2;
        int total = bucket_number * BUCKET_SIZE * 2;  
        printf("load factor is %d/%d = %f\n", cnt, total, float(cnt)/total);  
        pmem_memory_use = 2 * this->bucket_number * (max_pmem_cnt*(KEY_LEN+VAL_LEN)+PMEM_BITMAP_BYTESIZE) + (2*bucket_number*PMEM_BUCKET_SIZE)/8;
        cout<<"dram use: "<<(double)dram_memory_use/(1024*1024*1024)<<" pmem_use: "<<(double)pmem_memory_use/(1024*1024*1024)<<endl;
        // // 获取PMem访问统计信息  
        // double pmem_avg_time, pmem_max_time, pmem_min_time;  
        
        // // 输出PMem访问统计信息  
        // printf("\n--- PMem Access Statistics ---\n");  
        // printf("first kick:%d, total kick:%d, cnt1:%d, cnt2:%d\n",first_kick,total_kick,cnt1,cnt2);
        // printf("Total PMem Access Count: %d\n", pmem_access_count);   
        // printf("-----------------------------\n");  
        
        return 0;  
    }  
};  