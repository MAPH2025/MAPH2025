#include <iostream>
#include <cmath>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <thread>
#include "MapEmbed_pmem_thread.h"

using namespace std;

const string inputFilePath = "../../data/load30M.txt";
const string run_inputFilePath = "../../data/run30M-YCSBD-latest.txt";

KV_entry run_kvPairs[KV_NUM];

int basic_test(){
    printf("-------------------Begin basic_test-------------------\n");    
    int testcycles = 1;
/******************************* create MapEmbed ********************************/
    int layer = 3;
    int bucket_number = 1875000;//500000;
    int cell_number[3];
    cell_number[0] = 8437500;//2250000;
    cell_number[1] = 2812500;//750000;
    cell_number[2] = 937500;//250000;
    int cell_bit = 4;
    
    MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
    timespec time1, time2;
    long long resns = 0;

/****************************** create/read data *******************************/
    //create_random_kvs_keyint(kvPairs, KV_NUM);
    int p = 0;
    int fails = 0;
    int qfails = 0;
    int insertFailFlag = 0;
    int LFstepSum = 100;
    int LFstep[110];
    for (int i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * KV_NUM / LFstepSum);
/******************************** insert data **********************************/
    double insert_mops = 0;
    for(int i = 0; i < testcycles; ++i){
        resns = 0;
        MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
        clock_gettime(CLOCK_MONOTONIC, &time1); 
        for(int step = 0; step < LFstepSum; step++){
            auto start = std::chrono::high_resolution_clock::now();
            for(p = LFstep[step]; p < LFstep[step+1]; ++p){
                if(mapembed.insert(kvPairs[p]) == false)
                    if(++fails >= 8){
                        insertFailFlag = 1;
                        break;
                    }
                        
            }
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> diff = end - start;
            std::cout << "Time taken by MapEmbed Step " << step << " : " << diff.count() << " seconds" << std::endl;
            double mops = (LFstep[step+1]-LFstep[step]) / diff.count() / 1000000.0;
            std::cout << "1Performance: " << mops << " Mops (Million operations per second)" << std::endl;
            if(insertFailFlag) break;
        }
        clock_gettime(CLOCK_MONOTONIC, &time2); 
        resns += (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec); 
        double test_insert_mops = (double)1000.0 * p / resns; 
        insert_mops += (test_insert_mops - insert_mops) / (i+1);
    }

    // for(p = 0; p < KV_NUM; ++p){
    //     if(mapembed.insert(kvPairs[p]) == false)
    //         if(++fails >= 8)
    //             break;
    // }

/******************************* look-up data *********************************/
    // double query_mops = 0;
    // for(int i = 0; i < testcycles; ++i){
    //     resns = 0;
    //     clock_gettime(CLOCK_MONOTONIC, &time1); 
    //     for(int i = 0; i <= p; ++i){
    //         if(mapembed.query(kvPairs[i].key) == false)
    //             qfails++;
    //     }
    //     clock_gettime(CLOCK_MONOTONIC, &time2); 
    //     resns += (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec); 
    //     double test_query_mops = (double)1000.0 * p / resns; 
    //     query_mops += (test_query_mops - query_mops) / (i+1);
    // }

/******************************* print results *********************************/
    printf("inserted items: %d\n", mapembed.calculate_bucket_items());
    printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    // printf("insertion Mops: %lf, lookup Mops: %lf\n", insert_mops, query_mops);
    // printf("qfails: %d\n", qfails);

/***************************** dynamic expansion ******************************/
    // mapembed.extend();
    // fails = 0;
    // resns = 0;
    // int icnt = 0;
    // clock_gettime(CLOCK_MONOTONIC, &time1); 
    // for(; p < KV_NUM; ++p){
    //     if(mapembed.insert(kvPairs[p]) == false){
    //         if(++fails >= 8)
    //             break;
    //     }
    //     else icnt++;
    // }
    // clock_gettime(CLOCK_MONOTONIC, &time2); 
    // resns += (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec); 
    // insert_mops = (double)1000.0 * icnt / resns; 

/******************************* print results *********************************/
    // printf("-------------------after expansion-------------------\n");
    // printf("inserted items: %d\n", mapembed.calculate_bucket_items());
    // printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    // // printf("insertion Mops: %lf\n", insert_mops);
    // printf("-------------------End basic_test--------------------\n");
    return 0;
}

void read_ycsb_load(string load_path, KV_entry* kvPairs = kvPairs)
{
    std::ifstream inputFile(load_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::string line;
    int count = 0;
    while (std::getline(inputFile, line)) {
        // 查找包含 "usertable" 的行
        if (line.find("INSERT") != std::string::npos) {
            kvPairs[count].op = INSERT;
        } else if (line.find("READ") != std::string::npos) {
            kvPairs[count].op = READ;
        } else if (line.find("UPDATE") != std::string::npos) {
            kvPairs[count].op = UPDATE;
        } else if (line.find("DELETE") != std::string::npos) {
            kvPairs[count].op = DELETE;
        } else {
            // 如果找不到操作类型，跳过该行
            continue;
        }
        
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // 从 "user" 后面提取数字
            size_t userStart = found + strlen("usertable user"); // "user" 后面的位置
            size_t userEnd = line.find(" ", userStart); // 空格后面的位置

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    *(uint64_t*)kvPairs[count].key = userID;
                    *(uint64_t*)kvPairs[count++].value = 1;

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout<<"load: "<<count<<" keys"<<endl;
    
    inputFile.close();
}

int test_insert(){
    double res[10] = {0};
    std::ofstream res_file("./res_insert.csv");
    printf("-------------------TEST INSERT-------------------\n");    
    for(int t=0;t<10;++t){
    /******************************* create MapEmbed ********************************/
        int layer = 3;
        int bucket_number = KV_NUM/N;//500000;
        int cell_number[3];
        cell_number[0] = 15000000;//2250000;
        cell_number[1] = 7500000;//750000;
        cell_number[2] = 5000000;//250000;
        int cell_bit = 5;
        MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
    /****************************** create/read data *******************************/
        int p = 0;
        int fails = 0;
        // int qfails = 0;
        int insertFailFlag = 0;
        int LFstepSum = 100;
        int LFstep[110];
        for (int i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * KV_NUM / LFstepSum);
        //latency
        int lt_t = 10;
        chrono::duration<double> diff(0);
    /******************************** insert data **********************************/
        double insert_mops = 0;
        for(int step = 0; step < LFstepSum; step++){
            auto start = std::chrono::high_resolution_clock::now();
            for(p = LFstep[step]; p < LFstep[step+1]; p++){
                if(mapembed.insert(kvPairs[p]) == false){
                    if(++fails >= 1){
                        insertFailFlag = 1;
                        break;
                    }
                }      
            }
            auto end = std::chrono::high_resolution_clock::now();
            if(step == lt_t-1){
                diff += (end - start);
                mapembed.pmem_read_count = 0;
                mapembed.pmem_write_count = 0;
            }
            if(step == lt_t){
                cout<<"pmem read:"<<(double)mapembed.pmem_read_count/(LFstep[step+1] - LFstep[step])<<endl;
                cout<<"pmem write:"<<(double)mapembed.pmem_write_count/(LFstep[step+1] - LFstep[step])<<endl;
                cout << "Time taken by Cuckoo Step " << step << " : " << diff.count() << " seconds" << endl;
                diff += (end - start);
                //res_file << step << "," << diff.count()*1000000/(LFstep[step+1] - LFstep[step-1])<<endl;
                res[lt_t/10] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step-1]));
                diff = chrono::duration<double>(0);
                lt_t += 10;
            }
            // if(step>=90 && step<95){
            //     diff = end - start;
            //     res[step-90] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step]));
            // }
            if(insertFailFlag == 1) 
                break;
        }
        printf("inserted items: %d\n", mapembed.calculate_bucket_items());
        printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    }
    for(int i=0;i<=4;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
    return 0;
}

int test_read(){
    double res[10] = {0};
    std::ofstream res_file("./res_read.csv");
    printf("-------------------TEST READ-------------------\n");    
    for(int t=0;t<10;++t){
    /******************************* create MapEmbed ********************************/
        int layer = 3;
        int bucket_number = 1500000;//500000;
        int cell_number[3];
        cell_number[0] = 1500000;//2250000;
        cell_number[1] = 750000;//750000;
        cell_number[2] = 500000;//250000;
        int cell_bit = 4;
        
        MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
    /****************************** create/read data *******************************/
        int p = 0;
        int fails = 0;
        int qfails = 0;
        int insertFailFlag = 0;
        int LFstepSum = 100;
        int LFstep[110];
        for (int i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * KV_NUM / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
    /******************************** insert data **********************************/
        double insert_mops = 0;
        for(int step = 0; step < LFstepSum; step++){
            for(p = LFstep[step]; p < LFstep[step+1]; p++){
                if(mapembed.insert(kvPairs[p]) == false){
                    if(++fails >= 8){
                        insertFailFlag = 1;
                        break;
                    }
                }      
            }

            char result[VAL_LEN];
            char key[KEY_LEN] = "999";
            // if(step == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(p = LFstep[step]; p < LFstep[step+1]; p++){
            //         //mapembed.query(key,result);
            //         if(!mapembed.query(kvPairs[p].key,result)){
            //             qfails += 1;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(step == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(p = LFstep[step]; p < LFstep[step+1]; p++){
            //         //mapembed.query(key,result);
            //         if(!mapembed.query(kvPairs[p].key,result)){
            //             qfails += 1;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     cout << "Time taken by MapEmbed Step " << step << " : " << diff.count() << " seconds" << endl;
            //     //res_file << step << "," << diff.count()*1000000/(LFstep[step+1] - LFstep[step-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(step>=90 && step<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(p = LFstep[step]; p < LFstep[step+1]; p++){
                    mapembed.query(key,result);
                    // if(!mapembed.query(kvPairs[p].key,result)){
                    //     qfails += 1;
                    // }      
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[step-90] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step]));
            }
            if(insertFailFlag == 1) 
                break;
        }
        printf("inserted items: %d\n", mapembed.calculate_bucket_items());
        printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    }
    for(int i=0;i<5;++i){
        //res_file<<i*10<<","<<res[i]/10<<endl;
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
    return 0;
}


int test_update(){
    double res[10] = {0};
    std::ofstream res_file("./res_update.csv");
        printf("-------------------TEST UPDATE-------------------\n");    
    for(int t=0;t<10;++t){
    /******************************* create MapEmbed ********************************/
        int layer = 3;
        int bucket_number = 1500000;//500000;
        int cell_number[3];
        cell_number[0] = 15000000;//2250000;
        cell_number[1] = 7500000;//750000;
        cell_number[2] = 5000000;//250000;
        int cell_bit = 5;
        
        MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
    /****************************** create/read data *******************************/
        int p = 0;
        int fails = 0;
        int qfails = 0;
        int insertFailFlag = 0;
        int LFstepSum = 100;
        int LFstep[110];
        for (int i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * KV_NUM / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
    /******************************** insert data **********************************/
        double insert_mops = 0;
        for(int step = 0; step < LFstepSum; step++){
            for(p = LFstep[step]; p < LFstep[step+1]; p++){
                if(mapembed.insert(kvPairs[p]) == false){
                    if(++fails >= 8){
                        insertFailFlag = 1;
                        break;
                    }
                }      
            }            
            char result[VAL_LEN];

            // if(step == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[0]; u < LFstep[1]; u++){
            //         if(!mapembed.update(kvPairs[u])){
            //             cout<<"update fail"<<endl;
            //             qfails += 1;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(step == lt_t){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[0]; u < LFstep[1]; u++){
            //         if(!mapembed.update(kvPairs[u])){
            //             cout<<"update fail"<<endl;
            //             qfails += 1;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     cout << "Time taken by Cuckoo Step " << step << " : " << diff.count() << " seconds" << endl;
            //     diff += (end - start);
            //     //res_file << step << "," << diff.count()*1000000/(LFstep[step+1] - LFstep[step-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(step>=90 && step<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(int u = LFstep[step]; u < LFstep[step+1]; u++){
                    if(!mapembed.update(kvPairs[u])){
                        cout<<"update fail"<<endl;
                        qfails += 1;
                    }      
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[step-90] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step]));
            }
            if(insertFailFlag == 1) 
                break;
        }
        printf("inserted items: %d\n", mapembed.calculate_bucket_items());
        printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
    return 0;
}

int test_delete(){
    double res[10] = {0};
    std::ofstream res_file("./res_delete.csv");
    printf("-------------------TEST DELETE-------------------\n");    
    for(int t=0;t<10;++t){
    /******************************* create MapEmbed ********************************/
        int layer = 3;
        int bucket_number = 1500000;//500000;
        int cell_number[3];
        cell_number[0] = 15000000;//2250000;
        cell_number[1] = 7500000;//750000;
        cell_number[2] = 5000000;//250000;
        int cell_bit = 5;
        
        MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 1);
    /****************************** create/read data *******************************/
        int p = 0;
        int fails = 0;
        int qfails = 0;
        int insertFailFlag = 0;
        int LFstepSum = 100;
        int LFstep[110];
        for (int i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * KV_NUM / LFstepSum);
        int lt_t = 10;
        chrono::duration<double> diff(0);
    /******************************** insert data **********************************/
        double insert_mops = 0;
        for(int step = 0; step < LFstepSum; step++){
            for(p = LFstep[step]; p < LFstep[step+1]; p++){
                if(mapembed.insert(kvPairs[p]) == false){
                    if(++fails >= 8){
                        insertFailFlag = 1;
                        cout<<"insert fail"<<endl;
                        break;
                    }
                }      
            }

            // if(step == lt_t-1){
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[step]; u < LFstep[step+1]; u++){
            //         if(!mapembed.deletion(kvPairs[u].key)){
            //             // cout<<"delete fail"<<endl;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            // }
            // if(step == lt_t){
            //     cout << "Time taken by Cuckoo Step " << step << " : " << diff.count() << " seconds" << endl;
            //     auto start = std::chrono::high_resolution_clock::now();
            //     for(int u = LFstep[step]; u < LFstep[step+1]; u++){
            //         if(!mapembed.deletion(kvPairs[u].key)){
            //             // cout<<"delete fail"<<endl;
            //         }      
            //     }
            //     auto end = std::chrono::high_resolution_clock::now();
            //     diff += (end - start);
            //     //res_file << step << "," << diff.count()*1000000/(LFstep[step+1] - LFstep[step-1])<<endl;
            //     res[lt_t/10] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step-1]));
            //     diff = chrono::duration<double>(0);
            //     lt_t += 10;
            // }
            if(step>=90 && step<95){
                auto start = std::chrono::high_resolution_clock::now();
                for(int u = LFstep[step]; u < LFstep[step+1]; u++){
                    if(!mapembed.deletion(kvPairs[u].key)){
                        // cout<<"delete fail"<<endl;
                    }      
                }
                auto end = std::chrono::high_resolution_clock::now();
                diff = end - start;
                res[step-90] += (diff.count()*1000000/(LFstep[step+1] - LFstep[step]));
            }
            if(insertFailFlag == 1) 
                break;
        }
        printf("inserted items: %d\n", mapembed.calculate_bucket_items());
        printf("load factor: %lf, bits per key: %lf\n", mapembed.load_factor(), mapembed.bit_per_item());
    }
    // for(int i=1;i<10;++i){
    //     res_file<<i*10<<","<<res[i]/10<<endl;
    // }
    for(int i=0;i<5;++i){
        res_file<<i+91<<","<<res[i]/10<<endl;
    }
    res_file.close();
    return 0;
}

void multi_thread_insert(MapEmbed &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.insert(kvPairs[i]) == false){
            break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_read(MapEmbed &table, int begin, int end, chrono::duration<double>& time_cost){
    char result[VAL_LEN];
    uint64_t key;
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        // if(table.query(kvPairs[i].key,result) == false){
        //     cout<<"read fail: "<<i<<endl;
        //     //break;
        // }
        key = i;
        table.query((char *)(&key),result);
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_update(MapEmbed &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.update(kvPairs[i]) == false){
            cout<<"update fail"<<endl;
            break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_delete(MapEmbed &table, int begin, int end, chrono::duration<double>& time_cost){
    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        if(table.deletion(kvPairs[i].key) == false){
            cout<<"delete fail"<<endl;
            break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void multi_thread_op(MapEmbed &table, int begin, int end, chrono::duration<double>& time_cost){
    char result[VAL_LEN];

    auto t_start = std::chrono::high_resolution_clock::now();
    for(int i = begin; i < end; i++){
        KV_entry& entry = run_kvPairs[i]; 
        switch (entry.op) {
            case INSERT:
                table.insert(entry);
                break;
                
            case READ:
                table.query(entry.key, result);
                break;
                
            case UPDATE:
                table.update(entry);
                break;
                
            case DELETE:
                table.deletion(entry.key);
                break;
        }
    }
    auto t_end = std::chrono::high_resolution_clock::now();
    time_cost += (t_end - t_start);
}

void test_multi_threads(){
    ofstream res_file("./multi_ycsbd_latest.csv");
    double res[6] = {0};
    int thread_nums[6] = {1,2,4,8,16,24};
    for(int t=0;t<1;++t){
        for(int i=0;i<6;++i){
            int thread_num = thread_nums[i];
            std::vector<std::thread> threads;
            int layer = 3;
            int bucket_number = KV_NUM/N; //500000;
            int cell_number[3];
            cell_number[0] = 150000000;//2250000;
            cell_number[1] = 75000000;//750000;
            cell_number[2] = 5000000;//250000;  
            int cell_bit = 5;
            
            MapEmbed mapembed(layer, bucket_number, cell_number, cell_bit, 2);
            //prepare data
            cout<<"THREAD:"<<thread_num<<endl;
            int start1 = 0, start2 = 27000000;
            for(int j = start1 ;j < start2; j++){
                if(mapembed.insert(kvPairs[j]) == false){
                    cout<<"insert fail: "<<j<<endl;
                    //break;
                }
            }
            cout<<"Finish Prepare Data"<<endl;
            chrono::duration<double> total_time_cost(0);
            int total_re = 1500000;
            int record_per_thread = total_re/thread_num;
            for (int ii = 0; ii < thread_num; ++ii){
                threads.emplace_back(multi_thread_insert,std::ref(mapembed),ii * record_per_thread + start2,(ii+1) * record_per_thread + start2, std::ref(total_time_cost));
            }
            for (auto& t : threads) {
                t.join();
            }
            cout <<" load factor----: "<< mapembed.load_factor()<<endl;
            double mops = total_re / (total_time_cost.count()/thread_num) / 1000000.0;
            res[i] += mops;
            cout << "Performance: " << mops << " Mops (Million operations per second)" << endl;
            cout <<" load factor: "<< mapembed.load_factor()<<endl;
        } 
    }
    for(int i=0;i<6;++i){
        res_file<<thread_nums[i]<<","<<res[i]/2<<endl;
    }
    res_file.close();
}

void test_expansion(){
    ofstream res_file("./expansion.csv");
    int bucket_number = 5000000;
    int layer = 3;
    int cell_number[3];
    cell_number[0] = 15000000;//2250000;
    cell_number[1] = 7500000;//750000;
    cell_number[2] = 5000000;//250000;  
    int cell_bit = 5;
    
    MapEmbed mapembed(layer, bucket_number/N, cell_number, cell_bit, 2);
    for(int i = 0; i < 4500000; ++i){
        if(mapembed.insert(kvPairs[i]) == false){
            break;
        }
    }

    cout <<" load factor----: "<< mapembed.load_factor()<<endl;
    auto t_start = std::chrono::high_resolution_clock::now();
    mapembed.extend();
    auto t_end = std::chrono::high_resolution_clock::now();
    auto duration_time = std::chrono::duration<double>(t_end - t_start).count();
    cout <<" load factor====: "<< mapembed.load_factor()<<endl;
    res_file << bucket_number/1000000<<","<<duration_time<<endl;
}

int main(){
    //create_random_kvs_keyint(kvPairs, KV_NUM);
    read_ycsb_load(inputFilePath);
    // read_ycsb_load(run_inputFilePath,run_kvPairs);
    // test_multi_threads();
    test_multi_threads();
    return 0;
}