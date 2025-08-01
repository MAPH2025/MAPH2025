//g++ main.cpp -o main_pmem -lpmem -I. -std=c++17
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <string>
#include <thread>
#include "MAPH.h"

using namespace std;

#define TEST_SLOTS 30000000

// 定义操作类型枚举
enum OperationType {
    INSERT = 0,
    READ = 1,
    UPDATE = 2,
    DELETE = 3
};

struct inputEntry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

// 定义操作记录结构
struct Operation {
    OperationType type;
    Entry entry;
};

const string inputFilePath = "../data/load30M.txt";
const string run_inputFilePath = "../data/run30M-YCSBD-latest.txt";
vector<Operation> operations;  // 使用vector代替固定数组，更灵活
vector<Operation> run_ops;
int record_count = 0;

void read_ycsb_operations(string file_path, vector<Operation>& ops = operations)
{
    std::ifstream inputFile(file_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file: " << file_path << std::endl;
        return;
    }

    std::string line;
    record_count = 0;  // 重置计数器
    int insert_count = 0, read_count = 0, update_count = 0, delete_count = 0;
    while (std::getline(inputFile, line)) {
        Operation op;
        
        // 确定操作类型
        if (line.find("INSERT") != std::string::npos) {
            op.type = INSERT;
            insert_count++;
        } else if (line.find("READ") != std::string::npos) {
            op.type = READ;
            read_count++;
        } else if (line.find("UPDATE") != std::string::npos) {
            op.type = UPDATE;
            update_count++;
        } else if (line.find("DELETE") != std::string::npos) {
            op.type = DELETE;
            delete_count++;
        } else {
            // 如果找不到操作类型，跳过该行
            continue;
        }
        
        // 解析用户ID (键值)
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            size_t userStart = found + strlen("usertable user");
            size_t userEnd = line.find(" ", userStart);

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    op.entry.key = userID;
                    
                    // 如果是INSERT或UPDATE操作，设置值
                    char val[VAL_LEN] = "TEST";
                    if (op.type == INSERT || op.type == UPDATE) {
                        op.entry.val = *(uint64_t*)(val);
                    }
                    
                    ops.push_back(op);
                    record_count++;
                    
                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout << "Loaded " << record_count << " operations:" << endl;
    // cout << "  - INSERT: " << insert_count << endl;
    // cout << "  - READ: " << read_count << endl;
    // cout << "  - UPDATE: " << update_count << endl;
    // cout << "  - DELETE: " << delete_count << endl;
    
    inputFile.close();
}

void test_insert(int insert_number = TEST_SLOTS){
    ofstream res_file("./res_insert.csv");
    double res[10] = {0};
    for(int test_time=0;test_time<1;++test_time){
        CuckooHashTable table((insert_number/(2*BUCKET_SIZE)), 5);
        int LFstepSum = 100;
        int insertFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);

        //latency
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST INSERT------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            auto start = std::chrono::high_resolution_clock::now();
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                if(table.insert(operations[i].entry) == false){
                    printf("i:%d\n",i);
                    insertFailFlag = 1;
                    break;
                }
            }
            auto end = std::chrono::high_resolution_clock::now();

            if(st == lt_t-1){
                //table.pmem_read_count = 0;
                diff += (end - start);
            }
            if(st == lt_t){
                //cout<<"pmem_read_count:"<<(double)table.pmem_read_count/(LFstep[st+1] - LFstep[st])<<endl;
                cout << "Time taken by Cuckoo Step " << st << " : " << diff.count() << " seconds" << endl;
                diff += (end - start);
                // res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
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
        table.cal_load_factor();
    }
    for(int i=0;i<5;++i){
        // res_file<<i*10<<","<<res[i]/10<<endl;
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

void test_read(int insert_number = TEST_SLOTS){
    ofstream res_file("./res_read.csv");
    double res[10] = {0};
    for(int t=0;t<10;++t){
        CuckooHashTable table((insert_number/(2*BUCKET_SIZE)), 5);
        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST READ------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                if(table.insert(operations[i].entry) == false){
                    printf("insert fail i:%d\n",i);
                    insertFailFlag = 1;
                    break;
                }
            }
            table.clear_stage();
            char result[VAL_LEN];
            KEY_TYPE key = 999;

            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(i = LFstep[st]; i < LFstep[st+1]; ++i){
            //         // if(table.query(key,result) == false){
            //         //     // cout<<"Read Fail"<<endl;
            //         // }
            //         if(table.query(operations[i].entry.key,result) == false){
            //             printf("query fail i:%d, key:%ld\n",i,operations[i].entry.key);
            //             queryFailFlag = 1;
            //             break;
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(st == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(i = LFstep[st]; i < LFstep[st+1]; ++i){
            //         // if(table.query(key,result) == false){
            //         //     // cout<<"Read Fail"<<endl;
            //         // }
            //         if(table.query(operations[i].entry.key,result) == false){
            //             printf("query fail i:%d, key:%ld\n",i,operations[i].entry.key);
            //             queryFailFlag = 1;
            //             break;
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     cout << "Time taken by Cuckoo Step " << st << " : " << diff.count() << " seconds" << endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     //res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                    // if(table.query(key,result) == false){
                    //     // cout<<"Read Fail"<<endl;
                    // }
                    if(table.query(operations[i].entry.key,result) == false){
                        printf("query fail i:%d, key:%ld\n",i,operations[i].entry.key);
                        queryFailFlag = 1;
                        break;
                    }
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            }
            if(insertFailFlag) break;
        }
        table.cal_load_factor();
    }
    for(int i=0;i<5;++i){
        //res_file<<i*10<<","<<res[i]/10<<endl;
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

void test_update(int insert_number = TEST_SLOTS){
    ofstream res_file("./res_update.csv");
    double res[10] = {0};
    for(int t=0;t<10;++t){
        CuckooHashTable table((insert_number/(2*BUCKET_SIZE)), 5);
        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST UPDATE------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                if(table.insert(operations[i].entry) == false){
                    printf("insert fail i:%d\n",i);
                    insertFailFlag = 1;
                    break;
                }
            }
            int u;
            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         if(table.update(operations[u].entry) == false){
            //             cout<<"update fail"<<endl;
            //             break;
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(st == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         if(table.update(operations[u].entry) == false){
            //             cout<<"update fail"<<endl;
            //             break;
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     cout << "Time taken by Cuckoo Step " << st << " : " << diff.count() << " seconds" << endl;
            //     diff += (end - start);
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     // res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
                    if(table.update(operations[u].entry) == false){
                        cout<<"update fail"<<endl;
                        break;
                    }
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
            }
            if(insertFailFlag) break;
        }
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

void test_delete(int insert_number = TEST_SLOTS){
    ofstream res_file("./res_delete.csv");
    double res[10] = {0};
    for(int t=0;t<10;++t){
        CuckooHashTable table((insert_number/(2*BUCKET_SIZE)), 5);
        int LFstepSum = 100;
        int insertFailFlag = 0;
        int queryFailFlag = 0;
        int LFstep[110];
        int i=0;
        for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
        cout << "------TEST DELETE------" << endl;
        for(int st = 0; st < LFstepSum; st++){
            for(i = LFstep[st]; i < LFstep[st+1]; ++i){
                if(table.insert(operations[i].entry) == false){
                    printf("insert fail i:%d\n",i);
                    insertFailFlag = 1;
                    break;
                }
            }

            // if(st == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         if(table.deletion(operations[u].entry.key) == false){
            //             printf("delete fail\n");
            //             break;
            //         }
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         table.insert(operations[u].entry);
            //     }
            // }
            
            // if(st == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         if(table.deletion(operations[u].entry.key) == false){
            //             printf("delete fail\n");
            //             break;
            //         }
            //     }
            //     cout << "Time taken by Cuckoo Step " << st << " : " << diff.count() << " seconds" << endl;
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
            //         table.insert(operations[u].entry);
            //     }
            //     // res_file << st << "," << diff.count()*1000000/(LFstep[st+1] - LFstep[st-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(st>=90 && st<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
                    if(table.deletion(operations[u].entry.key) == false){
                        printf("delete fail\n");
                        break;
                    }
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[st-90] += (diff.count()*1000000/(LFstep[st+1] - LFstep[st]));
                for(int u = LFstep[st]; u < LFstep[st+1]; ++u){
                    table.insert(operations[u].entry);
                }
            }
            if(insertFailFlag) break;
        }
        table.cal_load_factor();
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
}

void test_consistency(){
    CuckooHashTable table((TEST_SLOTS/(2*BUCKET_SIZE)), 20);
    cout<<"consist: "<<table.pmem_table[0].bucket[12345].pair[3].key<<endl;
}

void multi_thread_insert(CuckooHashTable &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.insert(operations[i].entry) == false){
            cout<<"insert fail:"<<i<<endl;
            break;
        }else{
            if(i%10000000==0){
                cout<<i<<"--"<<operations[i].entry.key<<endl;
            }
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_read(CuckooHashTable &table, int begin, int end, chrono::duration<double>& time_cost){
    char res[VAL_LEN];
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        table.query((KEY_TYPE)i,res);
        // if(table.query(operations[i].entry.key,res) == false){
        //     cout<<"false read"<<endl;
        //     break;
        // }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_update(CuckooHashTable &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.update(operations[i].entry) == false){
            cout<<"false update"<<endl;
            break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_delete(CuckooHashTable &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.deletion(operations[i].entry.key) == false){
            cout<<"false update"<<endl;
            break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
    for(int i = begin; i < end; i++){
        table.insert(operations[i].entry);
    }
}

void multi_thread_op(CuckooHashTable &table, int begin, int end, chrono::duration<double>& time_cost){
    char result[VAL_LEN];

    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        Operation& op = run_ops[i]; 
        switch (op.type) {
            case INSERT:
                if(table.insert(op.entry)==false){
                    cout<<"IIIIII FFFFFFFFF"<<endl;
                }
                break;
                
            case READ:
                if(table.query(op.entry.key, result)==false){
                    //cout<<"QQQQQQQ FFFFFFFFF"<<op.entry.key<<endl;
                }
                break;
                
            case UPDATE:
                if(table.update(op.entry) == false){
                    cout<<"UUUUUUU FFFFFFFFF"<<endl;
                }
                break;
                
            case DELETE:
                table.deletion(op.entry.key);
                break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void test_multi_threads(){
    ofstream res_file("./lt.csv");
    double res[6] = {0};
    int thread_nums[6] = {1,2,4,8,16,24}; 
    for(int t=0;t<1;++t){
        for(int i=4;i<5;++i){
            int thread_num = thread_nums[i]; 
            std::vector<std::thread> threads;
            CuckooHashTable table((TEST_SLOTS/(2*BUCKET_SIZE)), 10);
            //prepare data
            cout<<"THREAD:"<<thread_num<<endl;
            int start1 = 0, start2 = 30000000;
            // for(int j = start1 ;j < start2; j++){
            //     if(table.insert(operations[j].entry) == false){
            //         break;
            //     }
            // }
            // cout<<"Finish Prepare Data"<<endl;
            int total_re = 28500000;
            int record_per_thread = total_re/thread_num;
            chrono::duration<double> total_time_cost(0);
            for (int j = 0; j < thread_num; ++j){
                threads.emplace_back(multi_thread_insert,std::ref(table),j * record_per_thread + start1,(j+1) * record_per_thread + start1, std::ref(total_time_cost));
            }
            for (auto& t : threads) {
                t.join();
            }
            cout<<"time:"<<total_time_cost.count()<<endl;
            double mops = total_re / (total_time_cost.count()/thread_num) / 1000000.0;
            res[i] += mops;
            int cnt = table.cal_load_factor();
            cout << "Performance: " << mops << " Mops (Million operations per second)" << endl;
            //res_file<<thread_num<<","<<lock_lt.count()/cnt<<","<<dram_lt.count()/cnt<<","<<pmem_lt.count()/cnt<<endl;
        }
    }
    // for(int i=0;i<6;++i){
    //     res_file<<thread_nums[i]<<","<<res[i]<<endl;
    // }
    res_file.close();
}

// void test_fp(){
//     ofstream res_file("./bucket.csv");
//     std::vector<std::thread> threads;
//     CuckooHashTable table((TEST_SLOTS/(2*BUCKET_SIZE)), 10);
//     //prepare data
//     int start1 = 0, start2 = 30000000;
//     int j;
//     for(j = start1 ;j < start2; j++){
//         if(table.insert(operations[j].entry) == false){
//             break;
//         }
//     }
//     table.pmem_read_count = 0;
//     char res[VAL_LEN];
//     for(int i = start1 ;i < j; i++){
//         if(table.query(operations[i].entry.key,res) == false){
//             break;
//         }
//     }

//     table.cal_load_factor();
//     cout << "conflict: " << (double)table.pmem_read_count / j << endl;
//     // res_file<<thread_num<<","<<mops<<endl;
//     res_file.close();
// }

void test_kick(){
    ofstream res_file("./kick.csv");
    int thread_num = 1; 
    int kick_lim = 10;
    std::vector<std::thread> threads;
    CuckooHashTable table((TEST_SLOTS/(2*BUCKET_SIZE)),kick_lim);
    //prepare data
    int start1 = 0, start2 = 30000000;
    int total_re = 30000000;
    int record_per_thread = total_re/thread_num;
    chrono::duration<double> total_time_cost(0);
    for (int j = 0; j < thread_num; ++j){
        threads.emplace_back(multi_thread_insert,std::ref(table),j * record_per_thread + start1,(j+1) * record_per_thread + start1, std::ref(total_time_cost));
    }
    for (auto& t : threads) {
        t.join();
    }
    double mops = total_re / (total_time_cost.count()/thread_num) / 1000000.0;
    table.cal_load_factor();
    cout << "Performance: " << mops << " Mops (Million operations per second)" << endl;
    res_file<<kick_lim<<","<<thread_num<<","<<mops<<endl;

    res_file.close();
}

// void test_recovery(){
//     PmemBucket* initial_pmem;
//     PmemBucket* initial_pmem2;
//     int i;
//     int test_slots = 25000000;
//     {
//         CuckooHashTable cuckoo((test_slots/(2*BUCKET_SIZE)), 10);
//         for(i = 0; i < 22500000; ++i){
//             if(cuckoo.insert(operations[i].entry) == false){
//                 break;
//             }
//         }
//         cuckoo.cal_load_factor();
//         initial_pmem = (PmemBucket* )malloc(cuckoo.bucket_number*sizeof(PmemBucket));
//         initial_pmem2 = (PmemBucket* )malloc(cuckoo.bucket_number*sizeof(PmemBucket));
//         memcpy(initial_pmem,cuckoo.pmem_table[0].bucket,cuckoo.bucket_number*sizeof(PmemBucket));
//         memcpy(initial_pmem2,cuckoo.pmem_table[1].bucket,cuckoo.bucket_number*sizeof(PmemBucket));
//     }
//     cout<<"finish init insert"<<endl;

//     CuckooHashTable cuckoo((test_slots/(2*BUCKET_SIZE)), 40, false);
//     auto r_start = std::chrono::high_resolution_clock::now();
//     cuckoo.recover();
//     auto r_end = std::chrono::high_resolution_clock::now();
//     chrono::duration<double> diff = r_end - r_start;
//     if(memcmp(cuckoo.pmem_table[0].bucket,initial_pmem,cuckoo.bucket_number*sizeof(PmemBucket))==0){
//         cout<<"same pmem"<<endl;
//     }else{
//         cout<<"pmem not match"<<endl;
//     }
//     cout<<"Recover time: "<<diff.count()<<endl;
//     cuckoo.cal_load_factor();
// }

void test_expansion(){
    ofstream res_file("./expansion.csv");
    int bucket_number = 25000000;
    CuckooHashTable cuckoo((bucket_number/(2*BUCKET_SIZE)), 10);
    for(int i = 0; i < 2250000; ++i){
        if(cuckoo.insert(operations[i].entry) == false){
            break;
        }
    }

    cuckoo.cal_load_factor();
    auto t_start = std::chrono::high_resolution_clock::now();
    cuckoo.expansion();
    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration_time = std::chrono::duration<double>(t_end - t_start).count();
    res_file << bucket_number/1000000<<","<<duration_time<<endl;
    cuckoo.cal_load_factor();
}

int main(){
    read_ycsb_operations(inputFilePath);
    // read_ycsb_operations(run_inputFilePath,run_ops);
    // test_multi_threads();
    test_expansion();
    return 0;

    int bucket_num = TEST_SLOTS/(2 * BUCKET_SIZE);
    printf("bucket num: %d\n",bucket_num);
    CuckooHashTable table(bucket_num, 3);
    int step;
    int total_ops = operations.size();

    //根据操作类型执行相应的表操作
    char result[VAL_LEN];
    bool success = false;
    auto start = std::chrono::high_resolution_clock::now();
    for (step = 0; step < total_ops; step++) {
        Operation& op = operations[step];
        
        switch (op.type) {
            case INSERT:
                success = table.insert(op.entry);
                break;
                
            case READ:
                success = table.query(op.entry.key, result);
                break;
                
            case UPDATE:
                success = table.update(op.entry);
                break;
                
            case DELETE:
                success = table.deletion(op.entry.key);  // 如果存在的话
                break;
        }
        
        if (!success) {
            printf("Operation failed at step %d, type: %d\n", step, op.type);
            break;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end - start;
    double execution_time = diff.count();
    double mops = step / execution_time / 1000000.0;
    
    cout << "Time taken by processing " << step << " operations: " << execution_time << " seconds" << endl;
    cout << "Performance: " << mops << " Mops (Million operations per second)" << endl;
    
    table.check_correct();
    table.cal_load_factor();
    return 0;
}