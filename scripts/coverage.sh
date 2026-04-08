#!/bin/bash
# FerrisDB Code Coverage Report
# Run: bash scripts/coverage.sh
# Requires: cargo-llvm-cov installed (cargo install cargo-llvm-cov)
#
# 输出：
#   stdout: 按文件覆盖率摘要
#   target/llvm-cov-html/: HTML 详细报告（可在浏览器中查看每行覆盖状态）

set -e

echo "=========================================="
echo " FerrisDB Code Coverage (llvm-cov)"
echo "=========================================="
echo ""

# 1. 收集覆盖数据并生成摘要
echo "[1/2] Running tests with coverage instrumentation..."
cargo llvm-cov test \
  -p ferrisdb-storage \
  -p ferrisdb-transaction \
  -p ferrisdb \
  --no-report \
  2>&1 | tail -3

echo ""
echo "[2/2] Generating report..."

# 摘要报告
echo ""
echo "=== Per-File Coverage ==="
cargo llvm-cov report --summary-only 2>&1 | grep -E 'ferrisdb.*/src/|TOTAL' | \
  awk '{
    file=$1;
    split($0, cols, " +");
    for(i=1;i<=NF;i++) { if(cols[i] ~ /%/) { pct=cols[i]; } }
    gsub(/.*crates\//, "", file);
    printf "  %-55s %s\n", file, pct;
  }'

echo ""
echo "=== Summary ==="
cargo llvm-cov report --summary-only 2>&1 | grep 'TOTAL'

# HTML 详细报告
echo ""
echo "Generating HTML report to target/llvm-cov-html/ ..."
cargo llvm-cov report --html --output-dir target/llvm-cov-html 2>&1 | tail -1
echo ""
echo "Open target/llvm-cov-html/index.html in browser for line-by-line coverage."
echo ""
echo "=== Coverage Targets ==="
echo "  Current: see TOTAL above"
echo "  Target:  >75% line coverage"
echo "  Critical files (<60%): recovery.rs, reader.rs, lru.rs, btree.rs"
