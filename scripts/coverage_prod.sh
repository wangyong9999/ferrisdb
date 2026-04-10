#!/bin/bash
# 生产代码覆盖率（排除 cfg(test) 块和 distributed 模块）
set -e

echo "=== Collecting coverage data ==="
cargo llvm-cov clean --workspace 2>/dev/null

# Core lib 分模块（避免合并 hang）
timeout 60 cargo llvm-cov test -p ferrisdb-core --lib --no-report -- \
    --skip "test_lwlock_concurrent" --skip "test_lwlock_shared_exclusive" 2>&1 | tail -1
timeout 30 cargo llvm-cov test -p ferrisdb-core --lib --no-report -- \
    "test_lwlock_concurrent" "test_lwlock_shared_exclusive" 2>&1 | tail -1

# Main packages
timeout 240 cargo llvm-cov test -p ferrisdb-storage -p ferrisdb-transaction -p ferrisdb \
    --no-report 2>&1 | tail -1

echo ""
echo "=== Production Code Coverage (excl cfg(test) + distributed) ==="
cargo llvm-cov report --json 2>/dev/null | python3 -c "
import json, sys, os

d = json.load(sys.stdin)
prod_covered = 0; prod_total = 0
test_covered = 0; test_total = 0

for f in d['data'][0]['files']:
    n = f['filename']
    if '/src/' not in n or 'bench' in n or 'distributed/' in n:
        continue
    l = f['summary']['lines']

    # Read source to find cfg(test) line ranges
    try:
        with open(n) as sf:
            src = sf.readlines()
    except:
        prod_covered += l['covered']; prod_total += l['count']
        continue

    in_test = False; depth = 0
    test_lines = set()
    for i, line in enumerate(src):
        s = line.strip()
        if s == '#[cfg(test)]':
            in_test = True; depth = 0
        if in_test:
            test_lines.add(i + 1)
            depth += s.count('{') - s.count('}')
            if depth <= 0 and s == '}' and len(test_lines) > 1:
                in_test = False

    # Estimate: test lines ratio in llvm-cov executable lines
    if len(src) > 0:
        test_ratio = len(test_lines) / len(src)
        est_test = int(l['count'] * test_ratio)
        est_test_cov = int(l['covered'] * min(test_ratio * 1.1, 1.0))  # test ~100% covered
        prod_total += l['count'] - est_test
        prod_covered += l['covered'] - est_test_cov
        test_total += est_test
        test_covered += est_test_cov

total = prod_total + test_total
total_cov = prod_covered + test_covered
print(f'  Overall (llvm-cov):     {100*total_cov/total:.1f}%  ({total_cov}/{total})')
print(f'  Production code only:   {100*prod_covered/prod_total:.1f}%  ({prod_covered}/{prod_total})')
print(f'  Test code (cfg(test)):  {100*test_covered/test_total:.1f}%  ({test_covered}/{test_total})')
print()
for target in [90, 95]:
    need = int(prod_total * target / 100) - prod_covered
    status = '✅' if need <= 0 else f'need {need} more'
    print(f'  Production {target}%: {status}')
"
