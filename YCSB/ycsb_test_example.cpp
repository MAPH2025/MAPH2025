#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <cstring>
#include <vector>
#include "Cuckoo.h" // 假设这是您的布谷鸟哈希表实现

using namespace std;

// 定义操作类型
enum OperationType {
    INSERT,
    READ,
    UPDATE,
    DELETE,
    SCAN
};

// 定义操作结构
struct Operation {
    OperationType type;
    uint64_t key;
    string value;
};

// 读取YCSB加载数据
void read_ycsb_load(const string& load_path, Entry* entries, int& count) {
    std::ifstream inputFile(load_path);

    if (!inputFile.is_open()) {
        std::cerr << "无法打开文件: " << load_path << std::endl;
        return;
    }

    std::string line;
    count = 0;

    // 跳过属性部分
    while (std::getline(inputFile, line)) {
        if (line.find("**********************************************") != std::string::npos) {
            break;
        }
    }

    // 读取数据部分
    while (std::getline(inputFile, line)) {
        // 查找包含 "usertable" 的行
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // 从 "user" 后面提取数字
            size_t userStart = found + strlen("usertable user"); // "user" 后面的位置
            size_t userEnd = line.find(" ", userStart); // 空格后面的位置

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    *(uint64_t*)entries[count].key = userID;
                    
                    // 提取字段值
                    size_t fieldStart = line.find("field0=", userEnd);
                    if (fieldStart != std::string::npos) {
                        fieldStart += 7; // "field0=" 的长度
                        size_t fieldEnd = line.find(" ]", fieldStart);
                        if (fieldEnd == std::string::npos) fieldEnd = line.find("]", fieldStart);
                        if (fieldEnd != std::string::npos) {
                            std::string fieldValue = line.substr(fieldStart, fieldEnd - fieldStart);
                            sprintf(entries[count].val, "%s", fieldValue.c_str());
                        }
                    } else {
                        sprintf(entries[count].val, "val%d", count);
                    }
                    
                    count++;
                } catch (const std::invalid_argument& e) {
                    std::cerr << "无效ID: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "ID超出范围: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout << "加载了 " << count << " 个键值对" << endl;
    inputFile.close();
}

// 读取YCSB运行数据（读/写操作）
vector<Operation> read_ycsb_run(const string& run_path) {
    vector<Operation> operations;
    std::ifstream inputFile(run_path);
    
    if (!inputFile.is_open()) {
        std::cerr << "无法打开文件: " << run_path << std::endl;
        return operations;
    }

    std::string line;
    
    // 跳过属性部分
    while (std::getline(inputFile, line)) {
        if (line.find("**********************************************") != std::string::npos) {
            break;
        }
    }

    // 读取操作部分
    while (std::getline(inputFile, line)) {
        Operation op;
        
        // 处理READ操作
        if (line.find("READ usertable user") == 0) {
            op.type = READ;
            size_t userStart = strlen("READ usertable user");
            std::string userIDStr = line.substr(userStart);
            op.key = std::stoull(userIDStr);
            operations.push_back(op);
        }
        // 处理UPDATE操作
        else if (line.find("UPDATE usertable user") == 0) {
            op.type = UPDATE;
            size_t userStart = strlen("UPDATE usertable user");
            size_t userEnd = line.find(" [", userStart);
            std::string userIDStr = line.substr(userStart, userEnd - userStart);
            op.key = std::stoull(userIDStr);
            
            // 提取字段值
            size_t fieldStart = line.find("field0=", userEnd);
            if (fieldStart != std::string::npos) {
                fieldStart += 7; // "field0=" 的长度
                size_t fieldEnd = line.find(" ]", fieldStart);
                if (fieldEnd == std::string::npos) fieldEnd = line.find("]", fieldStart);
                if (fieldEnd != std::string::npos) {
                    op.value = line.substr(fieldStart, fieldEnd - fieldStart);
                }
            }
            operations.push_back(op);
        }
        // 处理INSERT操作
        else if (line.find("INSERT usertable user") == 0) {
            op.type = INSERT;
            size_t userStart = strlen("INSERT usertable user");
            size_t userEnd = line.find(" [", userStart);
            std::string userIDStr = line.substr(userStart, userEnd - userStart);
            op.key = std::stoull(userIDStr);
            
            // 提取字段值
            size_t fieldStart = line.find("field0=", userEnd);
            if (fieldStart != std::string::npos) {
                fieldStart += 7; // "field0=" 的长度
                size_t fieldEnd = line.find(" ]", fieldStart);
                if (fieldEnd == std::string::npos) fieldEnd = line.find("]", fieldStart);
                if (fieldEnd != std::string::npos) {
                    op.value = line.substr(fieldStart, fieldEnd - fieldStart);
                }
            }
            operations.push_back(op);
        }
    }
    
    inputFile.close();
    return operations;
}

// 执行YCSB测试
void run_ycsb_test(const string& load_path, const string& run_path) {
    // 1. 加载数据
    Entry entries[100000]; // 假设最大支持10万条记录
    int count = 0;
    read_ycsb_load(load_path, entries, count);
    
    // 2. 创建哈希表并插入数据
    CuckooHashTable table(16384, 10); // 根据您的实现调整参数
    
    cout << "开始插入数据..." << endl;
    auto insert_start = std::chrono::high_resolution_clock::now();
    int inserted = 0;
    for (int i = 0; i < count; i++) {
        if (table.insert(entries[i])) {
            inserted++;
        } else {
            printf("插入失败，键: %llu\n", *(uint64_t*)entries[i].key);
            break;
        }
    }
    auto insert_end = std::chrono::high_resolution_clock::now();
    chrono::duration<double> insert_time = insert_end - insert_start;
    
    cout << "插入完成: " << inserted << "/" << count << " 条记录" << endl;
    cout << "插入时间: " << insert_time.count() << " 秒" << endl;
    cout << "插入吞吐量: " << inserted / insert_time.count() << " ops/s" << endl;
    
    // 3. 验证哈希表正确性
    table.check_correct();
    table.cal_load_factor();
    
    // 4. 读取运行操作
    vector<Operation> operations = read_ycsb_run(run_path);
    cout << "读取了 " << operations.size() << " 个操作" << endl;
    
    // 5. 执行操作并测量性能
    int read_success = 0, update_success = 0, insert_success = 0;
    auto run_start = std::chrono::high_resolution_clock::now();
    
    for (const auto& op : operations) {
        Entry entry;
        *(uint64_t*)entry.key = op.key;
        
        switch (op.type) {
            case READ: {
                if (table.lookup(entry)) {
                    read_success++;
                }
                break;
            }
            case UPDATE: {
                sprintf(entry.val, "%s", op.value.c_str());
                if (table.update(entry)) {
                    update_success++;
                }
                break;
            }
            case INSERT: {
                sprintf(entry.val, "%s", op.value.c_str());
                if (table.insert(entry)) {
                    insert_success++;
                }
                break;
            }
            default:
                break;
        }
    }
    
    auto run_end = std::chrono::high_resolution_clock::now();
    chrono::duration<double> run_time = run_end - run_start;
    
    // 6. 输出性能统计
    cout << "\n性能统计:" << endl;
    cout << "总操作数: " << operations.size() << endl;
    cout << "读操作成功: " << read_success << endl;
    cout << "更新操作成功: " << update_success << endl;
    cout << "插入操作成功: " << insert_success << endl;
    cout << "总执行时间: " << run_time.count() << " 秒" << endl;
    cout << "操作吞吐量: " << operations.size() / run_time.count() << " ops/s" << endl;
    
    // 7. 再次验证哈希表状态
    table.check_correct();
    table.cal_load_factor();
}

int main(int argc, char* argv[]) {
    string load_path = "load-64K.txt";
    string run_path = "run-64K.txt"; // 假设有一个运行文件
    
    // 如果命令行提供了参数，使用命令行参数
    if (argc > 1) load_path = argv[1];
    if (argc > 2) run_path = argv[2];
    
    cout << "使用加载文件: " << load_path << endl;
    cout << "使用运行文件: " << run_path << endl;
    
    run_ycsb_test(load_path, run_path);
    
    return 0;
}