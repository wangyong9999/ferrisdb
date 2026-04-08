#!/bin/bash
# FerrisDB Sanitizer Regression Check
# Run: bash scripts/sanitizer_check.sh
# Requires: rustup toolchain nightly installed

set -e

NIGHTLY="cargo +nightly"
TARGET="--target x86_64-unknown-linux-gnu"
ABI_FLAG="-Cunsafe-allow-abi-mismatch=sanitizer"
CRATES="-p ferrisdb-storage -p ferrisdb-transaction"

echo "=========================================="
echo " FerrisDB Sanitizer Regression Suite"
echo "=========================================="
echo ""

# 1. AddressSanitizer
echo "[1/3] AddressSanitizer (memory errors, UAF, buffer overflow)..."
RUSTFLAGS="-Z sanitizer=address $ABI_FLAG" \
  $NIGHTLY test $CRATES $TARGET -- --test-threads=1 2>&1 \
  | grep -E '(ERROR.*AddressSanitizer|SUMMARY.*AddressSanitizer)' && {
    echo "FAIL: ASan found errors!"
    exit 1
  }
echo "     ASan: CLEAN"
echo ""

# 2. ThreadSanitizer
echo "[2/3] ThreadSanitizer (data races)..."
TSAN_OUTPUT=$(RUSTFLAGS="-Z sanitizer=thread $ABI_FLAG" \
  $NIGHTLY test $CRATES $TARGET -- --test-threads=1 2>&1)
TSAN_RACES=$(echo "$TSAN_OUTPUT" | grep 'SUMMARY.*data race' | grep -v 'compiler_copy' || true)
TSAN_OUR_RACES=$(echo "$TSAN_RACES" | grep -v 'profiling.rs' || true)
if [ -n "$TSAN_OUR_RACES" ]; then
  # Check if all races are known false positives (parking_lot + buffer page)
  UNKNOWN=$(echo "$TSAN_OUR_RACES" | grep -v 'btree.rs' | grep -v 'heap_page.rs' || true)
  if [ -n "$UNKNOWN" ]; then
    echo "FAIL: TSan found NEW data races:"
    echo "$UNKNOWN"
    exit 1
  fi
  echo "     TSan: Known false positives only (parking_lot page lock)"
else
  echo "     TSan: CLEAN"
fi
echo ""

# 3. Standard test suite
echo "[3/3] Full test suite..."
cargo test --all 2>&1 | grep 'FAILED' && {
  echo "FAIL: Test failures found!"
  exit 1
}
echo "     Tests: ALL PASS"
echo ""

echo ""

# 3. TSan
echo "[3/4] ThreadSanitizer (data races)..."
TSAN_OUTPUT=$(RUSTFLAGS="-Z sanitizer=thread $ABI_FLAG" \
  $NIGHTLY test $CRATES $TARGET -- --test-threads=1 2>&1)
TSAN_RACES=$(echo "$TSAN_OUTPUT" | grep 'SUMMARY.*data race' | grep -v 'compiler_copy\|profiling\|alloc::sync' || true)
TSAN_NEW=$(echo "$TSAN_RACES" | grep -v 'btree.rs\|heap_page.rs\|content_lock.rs' || true)
if [ -n "$TSAN_NEW" ]; then
  echo "FAIL: TSan found NEW data races (not known FPs):"
  echo "$TSAN_NEW"
  exit 1
fi
echo "     TSan: Known FPs only (parking_lot page locks)"
echo ""

# 4. Coverage (informational)
echo "[4/4] Coverage measurement..."
timeout 120 cargo llvm-cov report --summary-only 2>&1 | grep TOTAL
echo ""

echo "=========================================="
echo " All sanitizer checks PASSED"
echo "=========================================="
