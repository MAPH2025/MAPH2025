# YCSB数据格式说明

## YCSB生成的数据格式

通过查看`load-64K.txt`文件，我们可以了解YCSB生成的数据格式。YCSB生成的数据主要包含以下部分：

### 1. 属性部分

文件开头是属性部分，包含了工作负载的各种配置参数：

```
***************** properties *****************
"insertproportion"="0"
"fieldcount"="1"
"fieldlength"="2"
"readproportion"="0.5"
"scanproportion"="0"
"readallfields"="true"
"dotransactions"="false"
"requestdistribution"="zipfian"
"workload"="site.ycsb.workloads.CoreWorkload"
"recordcount"="64000"
"updateproportion"="0.5"
"threads"="10"
"db"="site.ycsb.BasicDB"
"operationcount"="64000"
**********************************************
```

### 2. 数据记录部分

属性部分之后是数据记录，每行代表一条记录，格式如下：

```
INSERT usertable user6284781860667377211 [ field0=#H ]
INSERT usertable user8517097267634966620 [ field0=&< ]
INSERT usertable user1820151046732198393 [ field0=!4 ]
```

每条记录包含：
- 操作类型：如`INSERT`
- 表名：如`usertable`
- 键名：如`user6284781860667377211`
- 字段值：如`[ field0=#H ]`

### 3. 运行模式生成的数据

当执行`ycsb run`命令时，会生成包含READ、UPDATE等操作的数据：

```
READ usertable user6284781860667377211
UPDATE usertable user8517097267634966620 [ field0=新值 ]
```

## 与布谷鸟哈希表集成

您的代码中已经包含了`read_ycsb_load`函数来读取YCSB生成的数据：

```cpp
void read_ycsb_load(string load_path)
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
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // 从 "user" 后面提取数字
            size_t userStart = found + strlen("usertable user"); // "user" 后面的位置
            size_t userEnd = line.find(" ", userStart); // 空格后面的位置

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    *(uint64_t*)entry[count++].key = userID;

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
```

### 扩展建议

如果您需要处理更复杂的YCSB操作（如READ、UPDATE等），可以扩展现有的代码：

1. 创建一个函数来解析YCSB运行数据：

```cpp
void read_ycsb_run(string run_path, CuckooHashTable &table)
{
    std::ifstream inputFile(run_path);
    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::string line;
    int read_count = 0, update_count = 0;

    while (std::getline(inputFile, line)) {
        // 跳过属性部分
        if (line.find("*") != std::string::npos) continue;
        
        // 处理READ操作
        if (line.find("READ usertable user") != std::string::npos) {
            size_t userStart = line.find("user") + 4;
            std::string userIDStr = line.substr(userStart);
            uint64_t userID = std::stoull(userIDStr);
            
            Entry query;
            *(uint64_t*)query.key = userID;
            
            // 执行查询操作
            if (table.lookup(query)) {
                read_count++;
            }
        }
        // 处理UPDATE操作
        else if (line.find("UPDATE usertable user") != std::string::npos) {
            size_t userStart = line.find("user") + 4;
            size_t userEnd = line.find(" [", userStart);
            std::string userIDStr = line.substr(userStart, userEnd - userStart);
            uint64_t userID = std::stoull(userIDStr);
            
            Entry update_entry;
            *(uint64_t*)update_entry.key = userID;
            sprintf(update_entry.val, "updated");
            
            // 执行更新操作
            if (table.update(update_entry)) {
                update_count++;
            }
        }
    }
    
    cout << "Processed: " << read_count << " reads, " << update_count << " updates" << endl;
    inputFile.close();
}
```

2. 在主函数中调用此函数来处理运行数据：

```cpp
// 加载数据
read_ycsb_load(loadFilePath);
CuckooHashTable table = CuckooHashTable(16384, 10);

// 插入数据
for(int step=0; step<64000; step++) {
    if(!table.insert(entry[step])) {
        printf("insert fail %d\n", step);
        break;
    }
}

// 处理运行数据（读/写操作）
read_ycsb_run(runFilePath, table);
```

通过这种方式，您可以使用YCSB生成的各种操作来全面测试您的布谷鸟哈希表实现。