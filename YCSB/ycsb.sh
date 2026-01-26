#!/bin/bash
# ycsb.sh  —— YCSB 测试脚本（Linux 版）

YCSB_HOME="./ycsb-0.17.0"
WORKLOAD_FILE="./workload.properties"
OUTPUT_DIR="../ycsb_data"
mkdir -p "$OUTPUT_DIR"

echo "YCSB测试脚本"
echo "==============================="

echo "正在复制 workload.properties 到 YCSB 目录..."
cp -f "$WORKLOAD_FILE" "$YCSB_HOME/bin/workload.properties"

echo
echo "op:"
echo "1.  (load phase)"
echo "2.  (run phase)"
echo "3. quit"
read -p "input (1-3): " choice

case $choice in
  1)
    echo
    echo "loading..."
    cd "$YCSB_HOME/bin" || exit
    ./ycsb load basic -P ./workload.properties \
      > "$OUTPUT_DIR/basic_op/load2M.txt"
    echo "finish"
    ;;
  2)
    echo
    echo "runing..."
    cd "$YCSB_HOME/bin" || exit
    ./ycsb run basic -P ./workload.properties \
      > "$OUTPUT_DIR/basic_op/run2M_op_zipfian.txt"
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