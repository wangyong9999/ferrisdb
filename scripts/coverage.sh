#!/bin/bash
# FerrisDB Code Coverage Report
# Run: bash scripts/coverage.sh
# Requires: cargo-tarpaulin installed (cargo install cargo-tarpaulin)

set -e

echo "=========================================="
echo " FerrisDB Code Coverage Report"
echo "=========================================="
echo ""

# Run tarpaulin on core crates (exclude bench)
cargo tarpaulin \
  -p ferrisdb-storage \
  -p ferrisdb-transaction \
  -p ferrisdb \
  --skip-clean \
  --timeout 120 \
  --out stdout \
  2>&1 | tee /tmp/ferrisdb_coverage.txt

echo ""
echo "Coverage report saved to /tmp/ferrisdb_coverage.txt"
echo "Target: >70% line coverage"
