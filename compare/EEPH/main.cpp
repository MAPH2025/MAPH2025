#include <iostream>
#include <cmath>
#include <sys/stat.h>
#include <sys/time.h>
#include <fstream>
#include <cstring>
#include <gflags/gflags.h>

#include "Hash.h"
#include "EEPH.h"
#include "Dash/ex_finger.h"
#include "CCEH/CCEH_baseline.h"
#include "Level/level_baseline.h"
#include "libpmemobj.h"
#include "PMAllocator.h"
#include <chrono>

#define TEST_SLOTS 30000000

using namespace std;

typedef struct Record{
    enum Op : uint32_t {INSERT = 0, GET = 1, UPDATA = 2};
    Op op = INSERT;
    uint64_t key;
    uint64_t value;
} Record;

struct range {
  int index;
  uint64_t begin;
  uint64_t end;
  int length;
//   void *workload;
  std::vector<Record> *workload;
  uint64_t random_num;
  struct timeval tv;
};
 
static const char pool_name_eeph[] = "/mnt/pmem/cq-hash/pmem_eeph1.data";
static const char data_base_path[] = "../datasets/";
// std::filesystem::path dataset_path = "/home/cq/datasets/ycsb/raw_ycsb_wl_1090_uniform.dat";
static const size_t pool_size = 1024ul * 1024ul * 1024ul * 30ul;

DEFINE_string(index, "dash", "");
DEFINE_string(dataset, "ycsb_wl_1090_uniform.dat", "dataset name");
DEFINE_string(fv, "fixed", "fixed kv or variable kv");
DEFINE_uint32(thread, 1, "number of thread");

std::vector<Record> run_dataset;

namespace param
{
    std::string index_name, dataset_name, fv;
    uint32_t thread_num;
}

void set_affinity(uint32_t idx) {
  cpu_set_t my_set;
  CPU_ZERO(&my_set);
  if (idx < 24) {
    CPU_SET(idx, &my_set);
  } else {
    CPU_SET(idx + 24, &my_set);
  }
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
}

static bool file_exists(const char *pool_path)
{
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
}

void load_dataset(std::string wl_path, std::vector<Record> *data)
{
    std::cout << "Loading Dataset ..." << std::endl;
    std::cout << "path: " << wl_path << std::endl;
    std::ifstream ifs{wl_path, std::ios::binary | std::ios::ate};
    if(!ifs)
        std::cout << "Cannot open file: " << wl_path << std::endl;
    auto end = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    auto size = std::size_t(end - ifs.tellg());

    if(size == 0)
        std::cout << "empty file" << std::endl;
    const uint64_t number_records = size / sizeof(Record);
    data->resize(number_records);

    if(!ifs.read((char *)data->data(), size))
        std::cout << "Error reading from " << wl_path << std::endl;
    std::cout << "Loading Done. " << data->size() << " kvs are loaded." << std::endl;
    // std::cout << "records: " << cnt << std::endl;
}

template <class T>
void test_insert(struct range *range, Hash<T> *index)
{
    double res[10] = {0};
    ofstream res_file("./res_insert.csv");
    for(int t=0;t<10;++t){
        set_affinity(range->index);
        std::vector<Record> dataset = *range->workload;
        int insert_failed = 0, insert_success = 0, not_found = 0;
        int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;
        int ret;

        int LFstepSum = 100;
        int insertFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * TEST_SLOTS / LFstepSum);
        //latency
        int lt_t = 10;
        chrono::duration<double> diff(0);

        cout << "------TEST INSERT------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            auto start = std::chrono::high_resolution_clock::now();
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                if(index->Insert(dataset[i].key, (Value_t)1) != 1){
                    // printf("i:%d\n",i);
                    insert_failed ++;
                    if(insert_failed==8){
                        insertFailFlag = 1;
                        break;
                    }
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            if(st == lt_t-1){
                diff += (end - start);
                index->getPmemNumber(true);
            }
            if(st == lt_t){
                index->getPmemNumber(false);
                cout << "Time taken by EEPH Step " << st << " : " << diff.count() << " seconds" << endl;
                diff += (end - start);
                //res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
                res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
                diff = chrono::duration<double>(0);
                lt_t += 10;
            }
            // if(st>=90 && st<95){
            //     diff = end - start;
            //     res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            // }
            if(insertFailFlag) break;
        }
        index->getNumber();
        memset(index,0,sizeof(eeph::EEPH<T>));
        PMAllocator::Persist(index,sizeof(eeph::EEPH<T>));
        int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
        int cell_number = 8 * bucket_number;//2000000;
        int cell_hash = 16;
        new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
    }
    for(int i=0;i<5;++i){
        //res_file<<i*10<<","<<res[i]/10<<endl;
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

template <class T>
void test_read(struct range *range, Hash<T> *index)
{
    double res[10] = {0};
    ofstream res_file("./res_read.csv");
    for(int t=0;t<10;++t){
        set_affinity(range->index);
        std::vector<Record> dataset = *range->workload;
        int insert_failed = 0, insert_success = 0, not_found = 0;
        int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;
        int ret;

        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * TEST_SLOTS / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST READ------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                const Record& record = dataset[i];
                if(index->Insert(record.key, (Value_t)1) != 1){
                    cout<<"insert fail"<< i<<endl;
                    insert_failed ++;
                    if(insert_failed==8){
                        insertFailFlag = 1;
                        break;
                    }
                }
            }
            uint64_t key = 999;
            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int j = LFstep[st]; j < LFstep[st+1]; ++j){
            //         Value_t val = index->Get(key, true);
            //         // Value_t val = index->Get(dataset[j].key, true);
            //         // if(val == NONE){
            //         //     queryFailFlag = 1;
            //         //     break;
            //         // }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(st == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int j = LFstep[st]; j < LFstep[st+1]; ++j){
            //         Value_t val = index->Get(key, true);
            //         // Value_t val = index->Get(dataset[j].key, true);
            //         // if(val == NONE){
            //         //     queryFailFlag = 1;
            //         //     break;
            //         // }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     cout << "Time taken by EEPH Step " << st << " : " << diff.count() << " seconds" << endl;
            //     diff += (end - start);
            //     //res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                int j;
                for(j = LFstep[st]; j < LFstep[st+1]; ++j){
                    //Value_t val = index->Get(key, true);
                    Value_t val = index->Get(dataset[j].key, true);
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            }
            if(insertFailFlag) break;
        }
        index->getNumber();
        memset(index,0,sizeof(eeph::EEPH<T>));
        PMAllocator::Persist(index,sizeof(eeph::EEPH<T>));
        int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
        int cell_number = 8 * bucket_number;//2000000;
        int cell_hash = 16;
        new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        //res_file<<i*10<<","<<res[i]/10<<endl;
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}
template <class T>
void test_update(struct range *range, Hash<T> *index)
{
    double res[10] = {0};
    ofstream res_file("./res_update.csv");
    for(int t=0;t<10;++t){
        set_affinity(range->index);
        std::vector<Record> dataset = *range->workload;
        int insert_failed = 0, insert_success = 0, not_found = 0;
        int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;
        int ret;

        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * TEST_SLOTS / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST UPDATE------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                const Record& record = dataset[i];
                if(index->Insert(record.key, (Value_t)1) != 1){
                    insert_failed ++;
                    if(insert_failed==8){
                        insertFailFlag = 1;
                        break;
                    }
                }
            }

            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         Value_t val = index->Get(dataset[u].key, true);
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(st == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         Value_t val = index->Get(dataset[u].key, true);
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     cout << "Time taken by EEPH Step " << st << " : " << diff.count() << " seconds" << endl;
            //     diff += (end - start);
            //     // res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }

            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                int j;
                for(j = LFstep[st]; j < LFstep[st+1]; ++j){
                    Value_t val = index->Get(dataset[j].key, true);
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            }
            if(insertFailFlag) break;
        }
        index->getNumber();
        memset(index,0,sizeof(eeph::EEPH<T>));
        PMAllocator::Persist(index,sizeof(eeph::EEPH<T>));
        int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
        int cell_number = 8 * bucket_number;//2000000;
        int cell_hash = 16;
        new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

template <class T>
void test_delete(struct range *range, Hash<T> *index)
{
    double res[10] = {0};
    ofstream res_file("./res_delete.csv");
    for(int t=0;t<10;++t){
        set_affinity(range->index);
        std::vector<Record> dataset = *range->workload;
        int insert_failed = 0, insert_success = 0, not_found = 0;
        int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;
        int ret;

        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * TEST_SLOTS / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST DELETE------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                const Record& record = dataset[i];
                if(index->Insert(record.key, (Value_t)1) != 1){
                    insert_failed ++;
                    if(insert_failed==8){
                        insertFailFlag = 1;
                        break;
                    }
                }
            }

            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         const Record& record = dataset[u];
            //         if(index->Delete(record.key, true) == false){
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(st == lt_t){
            //     cout << "Time taken by EEPH Step " << st << " : " << diff.count() << " seconds" << endl;
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         const Record& record = dataset[u];
            //         if(index->Delete(record.key, true) == false){
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     //res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
                    const Record& record = dataset[u];
                    if(index->Delete(record.key, true) == false){
                    }
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            }
            if(insertFailFlag) break;
        }
        index->getNumber();
        memset(index,0,sizeof(eeph::EEPH<T>));
        PMAllocator::Persist(index,sizeof(eeph::EEPH<T>));
        int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
        int cell_number = 8 * bucket_number;//2000000;
        int cell_hash = 16;
        new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

template <class T>
void concurr_operation(struct range *range, Hash<T> *index)
{
    set_affinity(range->index);
    uint64_t begin = range->begin;
    uint64_t end = range->end;
    std::vector<Record> dataset = *range->workload;
    int insert_failed = 0, insert_success = 0, not_found = 0;
    int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;
    int ret;
    struct timespec start = {0, 0};
    struct timespec stop  = {0, 0};
    for(uint64_t i = begin; i < end; i++)
    {
        const Record& record = dataset[i];
        switch(record.op)
        {
            case Record::INSERT:
                ret = index->Insert(record.key, (Value_t)1);
                if(ret == 1)
                    insert_success ++;
                else{
                    insert_failed ++;
                }
                insert_cnt ++;
                break;
            case Record::GET:
                clock_gettime(CLOCK_REALTIME, &start);
                if(index->Get(record.key, true) == NONE)
                    not_found ++;
                clock_gettime(CLOCK_REALTIME, &stop);
                if(i % 150000 == 0)
                {
                    double latency = (double)(stop.tv_nsec - start.tv_nsec) +
                            (double)(stop.tv_sec - start.tv_sec) * 1000000000;
                    // std::cout << "Latency: " << (double)latency << "ns" << std::endl;
                }
                get_cnt ++;
                break;
            case Record::UPDATA:
                update_cnt ++;
                break;
            default:
                default_cnt ++;
        }
    }
    gettimeofday(&range->tv, NULL);
    std::cout << "insert failed: " << insert_failed << " insert success: " << insert_success 
        << std::endl;
    std::cout << "search not found: " << not_found << " search cnt: " << get_cnt << std::endl; 
    std::cout << "update cnt: " << update_cnt << " default cnt: " << default_cnt << std::endl;
}

template <class T>
void run_benchmark(std::vector<Record> *data, Hash<T> *index, uint64_t dataset_size,
        void (*test_func)(struct range *, Hash<T> *))
{
    uint32_t thread_num = 1;
    uint32_t chunk_size = dataset_size / thread_num;
    std::thread *thread_array[128];
    struct range *rarray = reinterpret_cast<range *>(malloc(thread_num * sizeof(struct range)));
    timeval tv1;
    double duration;
    for(uint32_t i = 0; i < thread_num; i++)
    {
        rarray[i].index = i;
        rarray[i].random_num = rand();
        rarray[i].begin = i * chunk_size;
        rarray[i].end = (i + 1) * chunk_size;
        rarray[i].length = 8;
        rarray[i].workload = data;
    }
    rarray[thread_num - 1].end = dataset_size;
    for(uint32_t i = 0; i < thread_num; i++)
    {
        thread_array[i] = new std::thread(*test_func, rarray + i, index);
    }
    gettimeofday(&tv1, NULL);
    for(uint32_t i = 0; i < thread_num; i ++)
    {
        thread_array[i]->join();
        delete thread_array[i];
    }
    double longest = (double)(rarray[0].tv.tv_usec - tv1.tv_usec) / 1000000 +
                   (double)(rarray[0].tv.tv_sec - tv1.tv_sec);
    double shortest = longest;
    duration = longest;

    for (int i = 1; i < thread_num; ++i) {
        double interval = (double)(rarray[i].tv.tv_usec - tv1.tv_usec) / 1000000 +
                        (double)(rarray[i].tv.tv_sec - tv1.tv_sec);
        duration += interval;
        if (shortest > interval) shortest = interval;
        if (longest < interval) longest = interval;
    }
    duration = duration / thread_num;
    double Mops = dataset_size / duration / 1000000;
    // std::cout << thread_num << " threads, Time = " << duration << " s, throughput = " << dataset_size / duration / 1000000
    //     << " Mops/s, fastest = " << dataset_size / shortest / 1000000 << ", slowest = " << dataset_size / longest / 1000000 
    //      << " Mops/s" << std::endl;
    // std::cout << "==========================================================" << std::endl;
}


template <class T>
void multi_thread_insert(struct range *range, Hash<T> *index)
{
    for(int i = range->begin; i < range->end; ++i){
        if(index->Insert((*range->workload)[i].key, (Value_t)1) != 1){
            break;
        }
    }
    gettimeofday(&range->tv, NULL);
}

template <class T>
void multi_thread_read(struct range *range, Hash<T> *index)
{
    for(int i = range->begin; i < range->end; ++i){
        index->Get((*range->workload)[i].key, true);
        // index->Get((uint64_t)i, true);
    }
    gettimeofday(&range->tv, NULL);
}

template <class T>
void multi_thread_delete(struct range *range, Hash<T> *index)
{
    for(int i = range->begin; i < range->end; ++i){
        index->Delete((*range->workload)[i].key, true);
    }
    gettimeofday(&range->tv, NULL);
}

template <class T>
void multi_thread_op(struct range *range, Hash<T> *index)
{
    for(int i = range->begin; i < range->end; ++i){
        Record& entry = run_dataset[i]; 
        switch (entry.op) {
            case entry.INSERT:
                index->Insert(entry.key, (Value_t)1);
                break;
                
            case entry.GET:
                index->Get(entry.key, false);
                break;
                
            case entry.UPDATA:
                index->Get(entry.key, true);
                break;
        }
    }
    gettimeofday(&range->tv, NULL);
}

template <class T>
void run_benchmark_multi(std::vector<Record> *data, Hash<T> *index, uint64_t dataset_size,
        void (*test_func)(struct range *, Hash<T> *))
{
    ofstream res_file("./multi-ycsbd-zipfian.csv");
    double res[6] = {0};
    int thread_nums[6] = {1,2,4,8,16,24};
    for(int t=0;t<1;++t){
        for(int i=3;i<4;++i){
            int start1 = 0, start2 = 28500000;
            // for(int j = start1 ;j < start2; j++){
            //     if(index->Insert((*data)[j].key, (Value_t)1) == false){
            //         break;
            //     }
            // }
            uint32_t thread_num = thread_nums[i];
            uint32_t total_re = 30000000;
            uint32_t chunk_size = total_re / thread_num;
            std::thread *thread_array[128];
            struct range *rarray = reinterpret_cast<range *>(malloc(thread_num * sizeof(struct range)));
            timeval tv1;
            double duration;
            for(uint32_t i = 0; i < thread_num; i++)
            {
                rarray[i].index = i;
                rarray[i].random_num = rand();
                rarray[i].begin = i * chunk_size + start1;
                rarray[i].end = (i + 1) * chunk_size + start1;
                rarray[i].length = 8;
                rarray[i].workload = data;
            }
            cout<<"finish prepare data"<<endl;
            for(uint32_t i = 0; i < thread_num; i++)
            {
                thread_array[i] = new std::thread(*test_func, rarray + i, index);
            }
            gettimeofday(&tv1, NULL);
            for(uint32_t i = 0; i < thread_num; i ++)
            {
                thread_array[i]->join();
                delete thread_array[i];
            }
            double longest = (double)(rarray[0].tv.tv_usec - tv1.tv_usec) / 1000000 +
                        (double)(rarray[0].tv.tv_sec - tv1.tv_sec);
            double shortest = longest;
            duration = longest;

            for (int i = 1; i < thread_num; ++i) {
                double interval = (double)(rarray[i].tv.tv_usec - tv1.tv_usec) / 1000000 +
                                (double)(rarray[i].tv.tv_sec - tv1.tv_sec);
                duration += interval;
                if (shortest > interval) shortest = interval;
                if (longest < interval) longest = interval;
            }
            duration = duration / thread_num;
            double Mops = total_re / duration / 1000000;
            res[i] += Mops;
            index->getNumber();
            memset(index,0,sizeof(eeph::EEPH<T>));
            PMAllocator::Persist(index,sizeof(eeph::EEPH<T>));
            int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
            int cell_number = 50 * bucket_number;//2000000;
            int cell_hash = 16;
            new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
            std::cout << thread_num << " threads, Time = " << duration << " s, throughput = " << Mops <<endl;
            std::cout << "==========================================================" << std::endl;
        }
    }
    for(int i=0;i<6;++i){
        res_file<<thread_nums[i]<<","<<res[i]/2<<endl;
    }
    res_file.close();
}

template <class T>
void test_expansion(std::vector<Record> *data, Hash<T> *index){
    ofstream res_file("./expansion.csv");
    int test_slots = 25000000;

    int bucket_number = test_slots / BUCKET_CAPACITY;//1000000;
    int cell_number = 4 * bucket_number;//2000000;
    int cell_hash = 16;
    new (index) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);
    for(int i = 0; i < 22500000; ++i){
        if(index->Insert((*data)[i].key, (Value_t)1) == false){
            break;
        }
    }
    index->getNumber();
    auto t_start = std::chrono::high_resolution_clock::now();
    index->testExpansion();
    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration_time = std::chrono::duration<double>(t_end - t_start).count();
    index->getNumber();
    res_file << test_slots/1000000<<","<<duration_time<<endl;
}

template <class T>
void test_recover(std::vector<Record> *data, Hash<T> *index, int test_slots){
    for(int i = 0; i < 22500000; ++i){
        if(index->Insert((*data)[i].key, (Value_t)1) == false){
            break;
        }
    }
    index->getNumber();
    auto t_start = std::chrono::high_resolution_clock::now();
    new (index) eeph::EEPH<T>();
    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration_time = std::chrono::duration<double>(t_end - t_start).count();
    index->getNumber();
    std::cout << test_slots/1000000<<","<<duration_time<<endl;
}

template <class T>
void init_index(std::string index_name, std::string dataset_name, std::string fv, uint64_t thread_num){
    std::vector<Record> dataset;
    Hash<uint64_t> *eh;
    bool pool_exist = false;
    // int insert_failed = 0, insert_success = 0, not_found = 0;
    // int insert_cnt = 0, get_cnt = 0, update_cnt = 0, default_cnt = 0;

    // load dataset
    const std::string tmp_path = data_base_path + std::string("load30M.dat");
    const std::string run_path = data_base_path + std::string("run30M-YCSBD-zipfian.dat");
    load_dataset(tmp_path, &dataset);
    load_dataset(run_path, &run_dataset);
    int dataset_size = dataset.size();
    std::cout << "============================= "<< index_name << " =============================" << std::endl;
    // load index
    int test_slots = 25000000;
    if(index_name == "eeph")
    {
        if(fv == "fixed")
        {
            if(file_exists(pool_name_eeph)) 
                pool_exist = true;
            PMAllocator::Initialize(pool_name_eeph, pool_size);
            eh = reinterpret_cast<Hash<T> *>(PMAllocator::GetRoot(sizeof(eeph::EEPH<T>)));
            if(!pool_exist)
            {
                int bucket_number = TEST_SLOTS / BUCKET_CAPACITY;//1000000;
                //int bucket_number = test_slots / BUCKET_CAPACITY; //test recovery
                int cell_number = 4 * bucket_number;//2000000;
                int cell_hash = 16;
                new (eh) eeph::EEPH<T>(bucket_number, cell_number, cell_hash);  
            }else
            {
                std::cout << "get existed eeph object" << std::endl;
                new (eh) eeph::EEPH<T>();
            }
        }else
        {
            /* variable kv */
        }
        
    }

    run_benchmark_multi(&dataset, eh, 0, &multi_thread_insert);
    //test_expansion(&dataset, eh);
    // test_recover(&dataset, eh, test_slots);
    //run_benchmark(&dataset, eh, KV_NUM, &test_insert);
    
    
    // insert operations
    // gettimeofday(&start, NULL);
    // for(int op_num = 0; op_num < dataset_size; op_num ++)
    // {
    //     const Record& record = dataset[op_num];
    //     switch(record.op)
    //     {
    //         case Record::INSERT:
    //             ret = eh->Insert(record.key, (Value_t)1);
    //             if(ret == 0)
    //                 insert_success ++;
    //             else
    //                 insert_failed ++;
    //             insert_cnt ++;
    //             break;
    //         case Record::GET:
    //             if(eh->Get(record.key, true) == NONE)
    //                 not_found ++;
    //             get_cnt ++;
    //             break;
    //         case Record::UPDATA:
    //             update_cnt ++;
    //             break;
    //         default:
    //             default_cnt ++;
    //     }
    // }
    // gettimeofday(&stop, NULL);
    // double insert_interval = (double)(stop.tv_usec - start.tv_usec) + 
    //                         (double)(stop.tv_sec - start.tv_sec) * 1000000;
    // std::cout << "throughput(Mops): " << (dataset_size - default_cnt) / insert_interval 
    // << " toal time: " << insert_interval / CLOCKS_PER_SEC << std::endl;
    // std::cout << "insert failed: " << insert_failed << " insert success: " << insert_success 
    //     << " total records: " << dataset.size() << std::endl;
    // std::cout << "search not found: " << not_found << " search cnt: " << get_cnt << std::endl; 
    // std::cout << "update cnt: " << update_cnt << " default cnt: " << default_cnt << std::endl;
    // std::cout << "============================= "<< index_name << " =============================" << std::endl;
}

int main(int argc, char* argv[]){
    google::ParseCommandLineFlags(&argc, &argv, true);
    param::index_name = FLAGS_index;
    param::dataset_name = FLAGS_dataset;
    param::fv = FLAGS_fv;
    param::thread_num = FLAGS_thread;
    init_index<uint64_t>(param::index_name, param::dataset_name, param::fv, param::thread_num);
    return 0;
}