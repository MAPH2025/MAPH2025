# YCSB 使用说明

## 问题分析

您在运行YCSB时遇到的错误：`Missing property: workload`，是因为workload.properties文件中缺少必要的`workload`属性。

## 解决方案

我已在项目目录中创建了一个示例`workload.properties`文件，包含所有必要的配置参数。您可以将此文件复制到YCSB的bin目录下使用。

### 配置文件说明

```properties
# YCSB Core Workload Properties file
# 指定工作负载类
workload=site.ycsb.workloads.CoreWorkload

# 数据库连接配置
db=site.ycsb.BasicDB

# 记录数量
recordcount=64000

# 操作数量
operationcount=64000

# 字段配置
fieldcount=1
fieldlength=2
readallfields=true

# 操作比例配置
readproportion=0.5
updateproportion=0.5
insertproportion=0
scanproportion=0

# 请求分布
requestdistribution=zipfian

# 线程数
threads=10

# 是否执行事务
dotransactions=false
```

### 使用方法

1. 将示例`workload.properties`文件复制到YCSB的bin目录：
   ```
   copy d:\Learning\Research\IIE\MirrorAsymmetryHash\pmem\workload.properties d:\Learning\Research\IIE\MirrorAsymmetryHash\data\ycsb-0.17.0\bin\
   ```

2. 执行数据加载命令：
   ```
   cd d:\Learning\Research\IIE\MirrorAsymmetryHash\data\ycsb-0.17.0\bin
   ycsb load basic -P ./workload.properties
   ```

3. 执行数据运行命令（生成读写操作）：
   ```
   ycsb run basic -P ./workload.properties
   ```

## 参数说明

- `workload`: 指定工作负载类，必须设置为`site.ycsb.workloads.CoreWorkload`
- `recordcount`: 要加载的记录数量
- `operationcount`: 要执行的操作数量
- `readproportion`: 读操作比例
- `updateproportion`: 更新操作比例
- `insertproportion`: 插入操作比例
- `scanproportion`: 扫描操作比例
- `requestdistribution`: 请求分布类型（zipfian、uniform、latest等）

## 自定义工作负载

您可以根据需要调整以下参数来生成不同类型的工作负载：

1. 读写比例调整：
   - 全读工作负载：`readproportion=1.0, updateproportion=0, insertproportion=0`
   - 全写工作负载：`readproportion=0, updateproportion=1.0, insertproportion=0`
   - 混合读写：`readproportion=0.5, updateproportion=0.5, insertproportion=0`

2. 数据量调整：
   - 修改`recordcount`和`operationcount`参数

3. 分布类型：
   - 均匀分布：`requestdistribution=uniform`
   - 偏斜分布：`requestdistribution=zipfian`
   - 最新分布：`requestdistribution=latest`

## 与您的哈希表测试集成

生成的数据文件（如load-64K.txt）可以直接用于您的布谷鸟哈希表测试。您的代码中已经包含了`read_ycsb_load`函数来读取这些数据。