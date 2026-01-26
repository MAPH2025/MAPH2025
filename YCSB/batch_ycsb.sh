#!/bin/bash
# batch_ycsb.sh  —— 批量生成 YCSB load & run 日志
# 用法：bash batch_ycsb.sh

set -e

YCSB_HOME="$HOME/MirrorAsymmetryHash/YCSB/ycsb-0.17.0"
OUTPUT_DIR="$HOME/MirrorAsymmetryHash/ycsb_data/temp_op"
RECORDCOUNT=200000000
OPCOUNT=200000000    
FIELD_COUNT=1
FIELD_LEN=2
WORKLOAD=site.ycsb.workloads.CoreWorkload

mkdir -p "$OUTPUT_DIR"
cd "$YCSB_HOME/bin"

echo "[1/13] generating load200M.txt ..."
./ycsb load basic \
  -p workload=$WORKLOAD \
  -p recordcount=$RECORDCOUNT \
  -p operationcount=$RECORDCOUNT \
  -p fieldcount=$FIELD_COUNT -p fieldlength=$FIELD_LEN \
  -p readproportion=0 -p updateproportion=0 -p insertproportion=1 -p scanproportion=0 \
  -p requestdistribution=uniform \
  > "$OUTPUT_DIR/load200M.txt"

############## 帮助函数：生成单个 run 文件 ##############
gen_run(){
  local name=$1      # 文件后缀名
  local readP=$2     # read 比例 0~1
  local otherP=$3    # insert 或 update 比例
  local otherOp=$4   # insert / update
  local dist=$5      # uniform / latest / zipfian
  local zipfConst=${6:-0.99}  # 只有 zipfian 时生效

  echo "[$(printf '%2d' $((++STEP))))/13] generating run_$name.txt ..."
  ./ycsb run basic \
    -p workload=$WORKLOAD \
    -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT \
    -p fieldcount=$FIELD_COUNT -p fieldlength=$FIELD_LEN \
    -p readproportion=$readP \
    -p ${otherOp}proportion=$otherP \
    -p insertproportion=$( [ "$otherOp" = "insert" ] && echo $otherP || echo 0 ) \
    -p updateproportion=$( [ "$otherOp" = "update" ] && echo $otherP || echo 0 ) \
    -p scanproportion=0 -p readmodifywriteproportion=0 \
    -p requestdistribution=$dist \
    $( [ "$dist" = "zipfian" ] && echo "-p zipfianconstant=$zipfConst" || echo ) \
    > "$OUTPUT_DIR/run_$name.txt"
}

STEP=1

for readP in 0.05 0.50 0.95; do
  # 用 awk 计算 1-readP，避免依赖 bc
  insertP=$(awk -v r=$readP 'BEGIN{printf "%.2f", 1-r}')
  gen_run "insert_${readP}_uniform" $readP $insertP insert uniform
  gen_run "insert_${readP}_latest"  $readP $insertP insert latest
done

for readP in 0.05 0.50 0.95; do
  updateP=$(awk -v r=$readP 'BEGIN{printf "%.2f", 1-r}')
  gen_run "update_${readP}_uniform" $readP $updateP update uniform
  gen_run "update_${readP}_zipfian" $readP $updateP update zipfian $zipfConst
done

echo "All done! Files are in $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"/run_*.txt