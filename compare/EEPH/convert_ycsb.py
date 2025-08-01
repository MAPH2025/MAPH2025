import sys
import struct
import re

# 定义操作类型
INSERT = 0
GET = 1
UPDATE = 2

def parse_ycsb_line(line):
    """解析YCSB数据行，返回操作类型和键"""
    if line.startswith("INSERT"):
        op = INSERT
        # 提取键值，格式为: INSERT usertable user<ID> [ field0=<value> ]
        match = re.search(r'user(\d+)', line)
        if match:
            key = int(match.group(1))
            return op, key, 0  # 值设为0
    elif line.startswith("READ"):
        op = GET
        match = re.search(r'user(\d+)', line)
        if match:
            key = int(match.group(1))
            return op, key, 0
    elif line.startswith("UPDATE"):
        op = UPDATE
        match = re.search(r'user(\d+)', line)
        if match:
            key = int(match.group(1))
            return op, key, 0
    return None

def convert_ycsb_file(input_file, output_file):
    """将YCSB文本文件转换为二进制格式"""
    with open(input_file, 'r') as f_in, open(output_file, 'wb') as f_out:
        # 跳过属性部分
        line = f_in.readline()
        while line and not line.startswith("INSERT") and not line.startswith("READ") and not line.startswith("UPDATE"):
            line = f_in.readline()
        
        # 处理数据行
        while line:
            result = parse_ycsb_line(line)
            if result:
                op, key, value = result
                # 写入二进制格式: op(4字节) + key(8字节) + value(8字节)
                f_out.write(struct.pack('IQQ', op, key, value))
            line = f_in.readline()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python convert_ycsb.py <输入文件> <输出文件>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_ycsb_file(input_file, output_file)
    print(f"转换完成: {input_file} -> {output_file}")