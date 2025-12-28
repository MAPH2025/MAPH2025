#!/bin/bash
# ycsb.sh  —— YCSB 测试脚本（Linux 版）

YCSB_HOME="/home/lab1806/MirrorAsymmetryHash/YCSB/ycsb-0.17.0"
WORKLOAD_FILE="/home/lab1806/MirrorAsymmetryHash/data/workload.properties"
OUTPUT_DIR="/home/lab1806/MirrorAsymmetryHash/data"
mkdir -p "$OUTPUT_DIR"

echo "YCSB测试脚本"
echo "==============================="

echo "正在复制 workload.properties 到 YCSB 目录..."
cp -f "$WORKLOAD_FILE" "$YCSB_HOME/bin/workload.properties"

echo
echo "选择操作:"
echo "1. 加载数据 (load)"
echo "2. 运行测试 (run)"
echo "3. 退出"
read -p "请输入选项 (1-3): " choice

case $choice in
  1)
    echo
    echo "loading..."
    cd "$YCSB_HOME/bin" || exit
    ./ycsb load basic -P ./workload.properties \
      > "$OUTPUT_DIR/load-90-for-zipfian.txt"
    echo "finish"
    ;;
  2)
    echo
    echo "runing..."
    cd "$YCSB_HOME/bin" || exit
    ./ycsb run basic -P ./workload.properties \
      > "$OUTPUT_DIR/run-read-zipfian-99.txt"
    echo "finish"
    ;;
  3)
    echo "exit"
    exit 0
    ;;
  *)
    echo "again"
    ;;
esac

echo