//! TPC-C Benchmark for FerrisDB (Rust)
//!
//! A real TPC-C implementation matching the C++ version at
//! dstore/tests/tpcctest/src/tpcc_client/tpcc_test_client.cpp
//!
//! Uses real HeapTable insert/update/delete/fetch/scan with MVCC transactions,
//! in-memory HashMap indexes for key lookups, and typed tuple serialization.

use clap::Parser;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BufferPool, BufferPoolConfig, StorageManager};
use ferrisdb_transaction::{HeapTable, TransactionManager, TupleId};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// TPCC Constants (matching C++ tpcc_table.h)
// ---------------------------------------------------------------------------

const DISTRICTS_PER_WAREHOUSE: u32 = 10;
const MAX_NUM_ITEMS: usize = 15; // max order lines per order

// Table OIDs
const TABLE_WAREHOUSE: u32 = 1;
const TABLE_DISTRICT: u32 = 2;
const TABLE_CUSTOMER: u32 = 3;
const TABLE_HISTORY: u32 = 4;
const TABLE_ORDER: u32 = 5;
const TABLE_NEW_ORDER: u32 = 6;
const TABLE_ORDER_LINE: u32 = 7;
const TABLE_ITEM: u32 = 8;
const TABLE_STOCK: u32 = 9;

// Index OIDs (offset by 100 to avoid collision with table OIDs)
const IDX_WAREHOUSE: u32 = 101;
const IDX_DISTRICT: u32 = 102;
const IDX_CUSTOMER: u32 = 103;
const IDX_ORDER: u32 = 105;
const IDX_NEW_ORDER: u32 = 106;
const IDX_ORDER_LINE: u32 = 107;
const IDX_ITEM: u32 = 108;
const IDX_STOCK: u32 = 109;

// ---------------------------------------------------------------------------
// Tuple Serialization
// ---------------------------------------------------------------------------

/// Column type tag for our simple serialization format.
#[repr(u8)]
enum ColType {
    Int32 = 1,
    Float64 = 3,
    String = 4,
    Timestamp = 5,
}

/// Builder for constructing serialized tuples with typed columns.
struct TupleBuilder {
    buf: Vec<u8>,
}

impl TupleBuilder {
    fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    fn put_int32(&mut self, val: i32) {
        self.buf.push(ColType::Int32 as u8);
        let len = 4u16.to_le_bytes();
        self.buf.extend_from_slice(&len);
        self.buf.extend_from_slice(&val.to_le_bytes());
    }

    fn put_float64(&mut self, val: f64) {
        self.buf.push(ColType::Float64 as u8);
        let len = 8u16.to_le_bytes();
        self.buf.extend_from_slice(&len);
        self.buf.extend_from_slice(&val.to_le_bytes());
    }

    fn put_string(&mut self, val: &str) {
        self.buf.push(ColType::String as u8);
        let bytes = val.as_bytes();
        let len = bytes.len() as u16;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(bytes);
    }

    fn put_timestamp(&mut self, val: i64) {
        self.buf.push(ColType::Timestamp as u8);
        let len = 8u16.to_le_bytes();
        self.buf.extend_from_slice(&len);
        self.buf.extend_from_slice(&val.to_le_bytes());
    }

    fn build(self) -> Vec<u8> {
        self.buf
    }
}

/// Reader for deserializing typed columns from a tuple.
struct TupleReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> TupleReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_int32(&mut self) -> Option<i32> {
        if self.pos + 3 > self.data.len() {
            return None;
        }
        let tag = self.data[self.pos];
        if tag != ColType::Int32 as u8 {
            return None;
        }
        self.pos += 1;
        let len = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
        self.pos += 2;
        if len != 4 || self.pos + 4 > self.data.len() {
            return None;
        }
        let val = i32::from_le_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Some(val)
    }

    fn read_float64(&mut self) -> Option<f64> {
        if self.pos + 3 > self.data.len() {
            return None;
        }
        let tag = self.data[self.pos];
        if tag != ColType::Float64 as u8 {
            return None;
        }
        self.pos += 1;
        let len = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
        self.pos += 2;
        if len != 8 || self.pos + 8 > self.data.len() {
            return None;
        }
        let val = f64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().ok()?);
        self.pos += 8;
        Some(val)
    }

    fn read_string(&mut self) -> Option<String> {
        if self.pos + 3 > self.data.len() {
            return None;
        }
        let _tag = self.data[self.pos];
        self.pos += 1;
        let len = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
        self.pos += 2;
        if self.pos + len > self.data.len() {
            return None;
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).to_string();
        self.pos += len;
        Some(s)
    }

    fn read_timestamp(&mut self) -> Option<i64> {
        if self.pos + 3 > self.data.len() {
            return None;
        }
        let tag = self.data[self.pos];
        if tag != ColType::Timestamp as u8 {
            return None;
        }
        self.pos += 1;
        let len = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
        self.pos += 2;
        if len != 8 || self.pos + 8 > self.data.len() {
            return None;
        }
        let val = i64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().ok()?);
        self.pos += 8;
        Some(val)
    }

    fn skip(&mut self) {
        if self.pos + 3 > self.data.len() {
            return;
        }
        self.pos += 1; // tag
        let len = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
        self.pos += 2 + len;
    }
}

// ---------------------------------------------------------------------------
// Sharded Index (fast concurrent point lookups + B-Tree for prefix scans)
// ---------------------------------------------------------------------------

const NUM_SHARDS: usize = 64;

/// Sharded concurrent HashMap for fast point lookups with optional B-Tree for prefix scans.
struct ShardedIndex {
    shards: Vec<parking_lot::RwLock<HashMap<Vec<u8>, TupleId>>>,
    btree: Option<Arc<BTree>>,
    needs_btree: bool,
}

impl ShardedIndex {
    fn new(index_oid: u32, buffer_pool: Arc<BufferPool>, needs_btree: bool) -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| parking_lot::RwLock::new(HashMap::new()))
            .collect();
        let btree = if needs_btree {
            Some(Arc::new(BTree::new(index_oid, buffer_pool)))
        } else {
            None
        };
        Self { shards, btree, needs_btree }
    }

    #[inline]
    fn shard_for(&self, key: &[u8]) -> usize {
        let mut h: usize = 0;
        for &b in key.iter().take(8) {
            h = h.wrapping_mul(31).wrapping_add(b as usize);
        }
        h % NUM_SHARDS
    }

    fn insert(&self, key: Vec<u8>, tid: TupleId) {
        let shard_idx = self.shard_for(&key);
        self.shards[shard_idx].write().insert(key.clone(), tid);
        if let Some(ref btree) = self.btree {
            let _ = btree.insert(
                BTreeKey::new(key),
                BTreeValue::Tuple { block: tid.ip_blkid, offset: tid.ip_posid },
            );
        }
    }

    fn get(&self, key: &[u8]) -> Option<TupleId> {
        let shard_idx = self.shard_for(key);
        self.shards[shard_idx].read().get(key).copied()
    }

    fn remove(&self, key: &[u8]) {
        let shard_idx = self.shard_for(key);
        self.shards[shard_idx].write().remove(key);
        if let Some(ref btree) = self.btree {
            let _ = btree.delete(&BTreeKey::new(key.to_vec()));
        }
    }

    fn find_min_with_prefix(&self, prefix: &[u8]) -> Option<(Vec<u8>, TupleId)> {
        if let Some(ref btree) = self.btree {
            match btree.find_min_with_prefix(prefix) {
                Ok(Some((key, BTreeValue::Tuple { block, offset }))) => {
                    return Some((key, TupleId::new(block, offset)));
                }
                _ => {}
            }
        }
        // Fallback: scan all shards
        let mut best: Option<(Vec<u8>, TupleId)> = None;
        for shard in &self.shards {
            let map = shard.read();
            for (k, v) in map.iter() {
                if k.starts_with(prefix) {
                    if best.is_none() || k < &best.as_ref().unwrap().0 {
                        best = Some((k.clone(), *v));
                    }
                }
            }
        }
        best
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, TupleId)> {
        if let Some(ref btree) = self.btree {
            if let Ok(results) = btree.scan_prefix(prefix) {
                let mapped: Vec<_> = results
                    .into_iter()
                    .filter_map(|(key, val)| {
                        if let BTreeValue::Tuple { block, offset } = val {
                            Some((key, TupleId::new(block, offset)))
                        } else {
                            None
                        }
                    })
                    .collect();
                if !mapped.is_empty() {
                    return mapped;
                }
            }
        }
        // Fallback: scan all shards
        let mut results: Vec<(Vec<u8>, TupleId)> = Vec::new();
        for shard in &self.shards {
            let map = shard.read();
            for (k, v) in map.iter() {
                if k.starts_with(prefix) {
                    results.push((k.clone(), *v));
                }
            }
        }
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }
}

/// Helper to build index keys.
fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for p in parts {
        buf.extend_from_slice(p);
        buf.push(0); // separator
    }
    buf
}

fn i32_key(v: i32) -> [u8; 4] {
    v.to_be_bytes()
}

// ---------------------------------------------------------------------------
// TPCC Tables (all 9 tables + indexes)
// ---------------------------------------------------------------------------

struct TpccTables {
    warehouse: Arc<HeapTable>,
    district: Arc<HeapTable>,
    customer: Arc<HeapTable>,
    history: Arc<HeapTable>,
    order: Arc<HeapTable>,
    new_order: Arc<HeapTable>,
    order_line: Arc<HeapTable>,
    item: Arc<HeapTable>,
    stock: Arc<HeapTable>,

    // Config values needed by transactions
    customer_per_district: u32,

    // B-Tree Indexes (sharded for fast point lookups + B-Tree for prefix scans)
    idx_warehouse: ShardedIndex,  // w_id -> tid
    idx_district: ShardedIndex,   // (w_id, d_id) -> tid
    idx_customer: ShardedIndex,   // (w_id, d_id, c_id) -> tid
    idx_item: ShardedIndex,       // i_id -> tid
    idx_stock: ShardedIndex,      // (w_id, i_id) -> tid
    idx_order: ShardedIndex,      // (w_id, d_id, o_id) -> tid
    idx_new_order: ShardedIndex,  // (w_id, d_id, o_id) -> tid
    idx_order_line: ShardedIndex, // (w_id, d_id, o_id, ol_number) -> tid
    idx_order_cust: ShardedIndex, // (w_id, d_id, c_id) -> (o_id, tid) for ORDER_STATUS
}

impl TpccTables {
    fn new(buffer_pool: Arc<BufferPool>, txn_mgr: Arc<TransactionManager>, customer_per_district: u32, wal_writer: Option<Arc<ferrisdb_storage::WalWriter>>, wal_ring: Option<Arc<ferrisdb_storage::wal::ring_buffer::WalRingBuffer>>) -> Self {
        let bp = &buffer_pool;
        let tm = &txn_mgr;
        let make_table = |oid: u32| -> Arc<HeapTable> {
            if let Some(ref w) = wal_writer {
                let mut t = HeapTable::with_wal_writer(oid, Arc::clone(bp), Arc::clone(tm), Arc::clone(w));
                if let Some(ref ring) = wal_ring {
                    t.set_wal_ring(Arc::clone(ring));
                }
                Arc::new(t)
            } else {
                Arc::new(HeapTable::new(oid, Arc::clone(bp), Arc::clone(tm)))
            }
        };
        Self {
            warehouse: make_table(TABLE_WAREHOUSE),
            district: make_table(TABLE_DISTRICT),
            customer: make_table(TABLE_CUSTOMER),
            history: make_table(TABLE_HISTORY),
            order: make_table(TABLE_ORDER),
            new_order: make_table(TABLE_NEW_ORDER),
            order_line: make_table(TABLE_ORDER_LINE),
            item: make_table(TABLE_ITEM),
            stock: make_table(TABLE_STOCK),
            customer_per_district,
            // B-Tree is disabled for now; prefix scans use shard fallback.
            // B-Tree insert overhead is too high for high-frequency tables.
            idx_warehouse: ShardedIndex::new(IDX_WAREHOUSE, Arc::clone(bp), false),
            idx_district: ShardedIndex::new(IDX_DISTRICT, Arc::clone(bp), false),
            idx_customer: ShardedIndex::new(IDX_CUSTOMER, Arc::clone(bp), false),
            idx_item: ShardedIndex::new(IDX_ITEM, Arc::clone(bp), false),
            idx_stock: ShardedIndex::new(IDX_STOCK, Arc::clone(bp), false),
            idx_order: ShardedIndex::new(IDX_ORDER, Arc::clone(bp), false),
            idx_new_order: ShardedIndex::new(IDX_NEW_ORDER, Arc::clone(bp), false),
            idx_order_line: ShardedIndex::new(IDX_ORDER_LINE, Arc::clone(bp), false),
            idx_order_cust: ShardedIndex::new(110, buffer_pool, false), // IDX 110 for order-by-cust
        }
    }

    /// 启用所有表的 WalBuffer 快速路径（数据加载完成后调用）
    /// 获取 WAL Ring Buffer 引用（第一个表的 ring）
    fn get_wal_ring(&self) -> Option<Arc<ferrisdb_storage::wal::ring_buffer::WalRingBuffer>> {
        // 所有表共享同一个 ring，从 warehouse 获取
        None // Ring 存在 TpccTables 外部，由 caller 管理
    }

    /// 禁用所有表的 WAL 写入（数据加载阶段）
    fn disable_wal(&self) {
        for t in [&self.warehouse, &self.district, &self.customer, &self.item,
                  &self.stock, &self.order, &self.new_order, &self.order_line, &self.history] {
            t.disable_wal();
        }
    }

    /// 启用所有表的 WAL 写入（benchmark 阶段）
    fn enable_wal(&self) {
        for t in [&self.warehouse, &self.district, &self.customer, &self.item,
                  &self.stock, &self.order, &self.new_order, &self.order_line, &self.history] {
            t.enable_wal();
        }
    }
}

// ---------------------------------------------------------------------------
// Random Helpers (matching C++ NURand, etc.)
// ---------------------------------------------------------------------------

fn random_number<R: Rng>(rng: &mut R, min: i32, max: i32) -> i32 {
    rng.gen_range(min..=max)
}

/// NURand from TPC-C spec. A=1023 for customer, A=8191 for items.
fn nu_rand<R: Rng>(rng: &mut R, a: u32, x: i32, y: i32) -> i32 {
    let c: i32 = rng.gen_range(0..a) as i32;
    ((random_number(rng, x, y) - x + c) % (y - x + 1)) + x
}

fn random_string<R: Rng>(rng: &mut R, len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..len).map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char).collect()
}

fn random_zip() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let zip_num: u32 = rng.gen_range(10000..99999);
    format!("{}11111", zip_num / 100000)
}

fn get_other_warehouse_id<R: Rng>(rng: &mut R, w_id: i32, num_warehouses: i32) -> i32 {
    if num_warehouses <= 1 {
        return w_id;
    }
    let mut w;
    loop {
        w = random_number(rng, 1, num_warehouses);
        if w != w_id {
            return w;
        }
    }
}

// ---------------------------------------------------------------------------
// Data Loading (matching C++ GenDataInto*)
// ---------------------------------------------------------------------------

/// Load all TPCC data for the given configuration.
fn load_tpcc_data(tables: &TpccTables, num_warehouses: u32, item_num: u32, customer_per_district: u32, order_per_district: u32) {
    let load_xid = Xid::new(0, 1); // special load transaction xid

    println!("Loading ITEM table ({} items)...", item_num);
    for i_id in 1..=item_num {
        let i_name = format!("item_{}", i_id);
        let i_data = random_string(&mut StdRng::seed_from_u64(i_id as u64), 50);
        let i_price = (i_id as f64 % 100.0) + 1.0;
        let i_im_id = ((i_id % 10000) + 1) as i32;

        let mut b = TupleBuilder::with_capacity(128);
        b.put_int32(i_id as i32);         // I_ID
        b.put_string(&i_name);            // I_NAME
        b.put_float64(i_price);           // I_PRICE
        b.put_string(&i_data);            // I_DATA
        b.put_int32(i_im_id);             // I_IM_ID

        let data = b.build();
        let tid = tables.item.insert(&data, load_xid, 0).unwrap();
        tables.idx_item.insert(make_key(&[&i32_key(i_id as i32)]), tid);
    }
    println!("  ITEM loaded: {} rows", item_num);

    for w_id in 1..=num_warehouses {
        let seed = (w_id as u64) * 1000;
        let mut rng = StdRng::seed_from_u64(seed);

        // WAREHOUSE
        {
            let w_name = format!("warehouse_{}", w_id);
            let w_street_1 = format!("w{}_street_1", w_id);
            let w_street_2 = format!("w{}_street_2", w_id);
            let w_city = format!("w{}_city", w_id);
            let w_state = "CA";
            let w_zip = random_zip();
            let w_tax: f64 = rng.gen_range(0.0..0.2);
            let w_ytd: f64 = 300000.0;

            let mut b = TupleBuilder::with_capacity(200);
            b.put_int32(w_id as i32);      // W_ID
            b.put_float64(w_ytd);          // W_YTD
            b.put_float64(w_tax);          // W_TAX
            b.put_string(&w_name);         // W_NAME
            b.put_string(&w_street_1);     // W_STREET_1
            b.put_string(&w_street_2);     // W_STREET_2
            b.put_string(&w_city);         // W_CITY
            b.put_string(w_state);         // W_STATE
            b.put_string(&w_zip);          // W_ZIP

            let data = b.build();
            let tid = tables.warehouse.insert(&data, load_xid, 0).unwrap();
            tables.idx_warehouse.insert(make_key(&[&i32_key(w_id as i32)]), tid);
        }

        // DISTRICT (10 per warehouse)
        for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
            let d_name = format!("district_{}_{}", w_id, d_id);
            let d_street_1 = format!("d{}_{}_st1", w_id, d_id);
            let d_street_2 = format!("d{}_{}_st2", w_id, d_id);
            let d_city = format!("d{}_{}_city", w_id, d_id);
            let d_state = "CA";
            let d_zip = random_zip();
            let d_tax: f64 = rng.gen_range(0.0..0.2);
            let d_ytd: f64 = 30000.0;
            let d_next_o_id = order_per_district as i32 + 1;

            let mut b = TupleBuilder::with_capacity(200);
            b.put_int32(d_id as i32);          // D_ID
            b.put_int32(w_id as i32);          // D_W_ID
            b.put_float64(d_ytd);              // D_YTD
            b.put_float64(d_tax);              // D_TAX
            b.put_int32(d_next_o_id);          // D_NEXT_O_ID
            b.put_string(&d_name);             // D_NAME
            b.put_string(&d_street_1);         // D_STREET_1
            b.put_string(&d_street_2);         // D_STREET_2
            b.put_string(&d_city);             // D_CITY
            b.put_string(d_state);             // D_STATE
            b.put_string(&d_zip);              // D_ZIP

            let data = b.build();
            let tid = tables.district.insert(&data, load_xid, 0).unwrap();
            tables.idx_district.insert(
                make_key(&[&i32_key(w_id as i32), &i32_key(d_id as i32)]),
                tid,
            );
        }

        // STOCK (item_num per warehouse)
        for s_i_id in 1..=item_num {
            let s_quantity = random_number(&mut rng, 10, 100);
            let s_data = random_string(&mut rng, 50);

            let mut b = TupleBuilder::with_capacity(512);
            b.put_int32(w_id as i32);          // S_W_ID
            b.put_int32(s_i_id as i32);        // S_I_ID
            b.put_int32(s_quantity);            // S_QUANTITY
            b.put_int32(0);                     // S_YTD
            b.put_int32(0);                     // S_ORDER_CNT
            b.put_int32(0);                     // S_REMOTE_CNT
            b.put_string(&s_data);              // S_DATA
            for d in 1..=10u32 {
                b.put_string(&format!("s_dist_{:02}", d));
            }

            let data = b.build();
            let tid = tables.stock.insert(&data, load_xid, 0).unwrap();
            tables.idx_stock.insert(
                make_key(&[&i32_key(w_id as i32), &i32_key(s_i_id as i32)]),
                tid,
            );
        }

        // CUSTOMER (customer_per_district per district, per warehouse)
        // HISTORY (one per customer)
        for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
            for c_id in 1..customer_per_district {
                let c_first = format!("first_{}_{}_{}", w_id, d_id, c_id);
                let c_middle = "OE";
                let c_last = format!("last_{}_{}_{}", w_id, d_id, c_id);
                let c_street_1 = format!("c{}_{}_{}_st1", w_id, d_id, c_id);
                let c_street_2 = format!("c{}_{}_{}_st2", w_id, d_id, c_id);
                let c_city = format!("c{}_{}_{}_city", w_id, d_id, c_id);
                let c_state = "CA";
                let c_zip = random_zip();
                let c_since: i64 = 1000000;
                let c_credit = if c_id % 2 == 0 { "GC" } else { "BC" };
                let c_credit_lim: f64 = 50000.0;
                let c_discount: f64 = rng.gen_range(0.0..0.5);
                let c_balance: f64 = -10.0;
                let c_ytd_payment: f64 = 10.0;
                let c_payment_cnt: i32 = 1;
                let c_delivery_cnt: i32 = 0;
                let c_data = random_string(&mut rng, 200);

                let mut b = TupleBuilder::with_capacity(600);
                b.put_int32(w_id as i32);       // C_W_ID
                b.put_int32(d_id as i32);       // C_D_ID
                b.put_int32(c_id as i32);       // C_ID
                b.put_float64(c_discount);      // C_DISCOUNT
                b.put_string(c_credit);         // C_CREDIT
                b.put_string(&c_last);          // C_LAST
                b.put_string(&c_first);         // C_FIRST
                b.put_float64(c_credit_lim);    // C_CREDIT_LIM
                b.put_float64(c_balance);       // C_BALANCE
                b.put_float64(c_ytd_payment);   // C_YTD_PAYMENT
                b.put_int32(c_payment_cnt);     // C_PAYMENT_CNT
                b.put_int32(c_delivery_cnt);    // C_DELIVERY_CNT
                b.put_string(&c_street_1);      // C_STREET_1
                b.put_string(&c_street_2);      // C_STREET_2
                b.put_string(&c_city);          // C_CITY
                b.put_string(c_state);          // C_STATE
                b.put_string(&c_zip);           // C_ZIP
                b.put_string(&random_string(&mut rng, 16)); // C_PHONE
                b.put_timestamp(c_since);       // C_SINCE
                b.put_string(c_middle);         // C_MIDDLE
                b.put_string(&c_data);          // C_DATA

                let data = b.build();
                let tid = tables.customer.insert(&data, load_xid, 0).unwrap();
                tables.idx_customer.insert(
                    make_key(&[&i32_key(w_id as i32), &i32_key(d_id as i32), &i32_key(c_id as i32)]),
                    tid,
                );

                // HISTORY row for each customer
                let h_id = ((w_id - 1) as i64) * 30000 + ((d_id - 1) as i64) * 3000 + c_id as i64;
                let mut hb = TupleBuilder::with_capacity(128);
                hb.put_int32(h_id as i32);       // H_ID
                hb.put_int32(c_id as i32);       // H_C_ID
                hb.put_int32(d_id as i32);       // H_C_D_ID
                hb.put_int32(w_id as i32);       // H_C_W_ID
                hb.put_int32(d_id as i32);       // H_D_ID
                hb.put_int32(w_id as i32);       // H_W_ID
                hb.put_timestamp(c_since);       // H_DATE
                hb.put_float64(10.0);            // H_AMOUNT
                hb.put_string("mon");            // H_DATA

                let h_data = hb.build();
                let _ = tables.history.insert(&h_data, load_xid, 0);
            }
        }

        // ORDER + NEW_ORDER + ORDER_LINE (matching C++ GenDataIntoOrder)
        // Shuffle customer IDs for random permutation
        let mut customer_ids: Vec<u32> = (1..=customer_per_district).collect();
        let mut shuffle_rng = StdRng::seed_from_u64(seed + 42);
        customer_ids.shuffle(&mut shuffle_rng);

        let new_order_start = (order_per_district as f64 * 0.7).ceil() as u32 + 1;

        for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
            for o_id in 1..=order_per_district {
                let c_id = customer_ids[((o_id - 1) % customer_per_district) as usize];
                let ol_cnt = random_number(&mut rng, 5, MAX_NUM_ITEMS as i32);
                let all_local = 1i32;
                let o_entry_d: i64 = 1000000 + o_id as i64;

                let is_new = o_id >= new_order_start;
                let o_carrier_id: i32 = if is_new { 0 } else { d_id as i32 };

                // ORDER row
                let mut ob = TupleBuilder::with_capacity(128);
                ob.put_int32(o_id as i32);         // O_ID
                ob.put_int32(d_id as i32);         // O_D_ID
                ob.put_int32(w_id as i32);         // O_W_ID
                ob.put_int32(c_id as i32);         // O_C_ID
                ob.put_int32(o_carrier_id);        // O_CARRIER_ID
                ob.put_int32(ol_cnt);              // O_OL_CNT
                ob.put_int32(all_local);           // O_ALL_LOCAL
                ob.put_timestamp(o_entry_d);       // O_ENTRY_D

                let o_data = ob.build();
                let tid = tables.order.insert(&o_data, load_xid, 0).unwrap();
                tables.idx_order.insert(
                    make_key(&[&i32_key(w_id as i32), &i32_key(d_id as i32), &i32_key(o_id as i32)]),
                    tid,
                );
                tables.idx_order_cust.insert(
                    make_key(&[&i32_key(w_id as i32), &i32_key(d_id as i32), &i32_key(c_id as i32)]),
                    tid,
                );

                // NEW_ORDER row (only for undelivered orders)
                if is_new {
                    let mut nb = TupleBuilder::with_capacity(64);
                    nb.put_int32(o_id as i32);    // NO_O_ID
                    nb.put_int32(d_id as i32);    // NO_D_ID
                    nb.put_int32(w_id as i32);    // NO_W_ID

                    let n_data = nb.build();
                    let tid = tables.new_order.insert(&n_data, load_xid, 0).unwrap();
                    tables.idx_new_order.insert(
                        make_key(&[&i32_key(w_id as i32), &i32_key(d_id as i32), &i32_key(o_id as i32)]),
                        tid,
                    );
                }

                // ORDER_LINE rows
                for ol_number in 1..=ol_cnt as u32 {
                    let ol_i_id = random_number(&mut rng, 1, item_num as i32);
                    let ol_supply_w_id = w_id as i32;
                    let ol_quantity: i32 = 5;
                    let ol_amount = if is_new {
                        rng.gen_range(1.0..9999.99)
                    } else {
                        0.0
                    };
                    let ol_delivery_d = if is_new { 0i64 } else { o_entry_d };

                    let mut olb = TupleBuilder::with_capacity(128);
                    olb.put_int32(w_id as i32);         // OL_W_ID
                    olb.put_int32(d_id as i32);         // OL_D_ID
                    olb.put_int32(o_id as i32);         // OL_O_ID
                    olb.put_int32(ol_number as i32);    // OL_NUMBER
                    olb.put_int32(ol_i_id);              // OL_I_ID
                    olb.put_int32(ol_supply_w_id);       // OL_SUPPLY_W_ID
                    olb.put_timestamp(ol_delivery_d);    // OL_DELIVERY_D
                    olb.put_int32(ol_quantity);           // OL_QUANTITY
                    olb.put_float64(ol_amount);           // OL_AMOUNT
                    olb.put_string(&format!("ol_dist_{}", ol_number)); // OL_DIST_INFO

                    let ol_data = olb.build();
                    let tid = tables.order_line.insert(&ol_data, load_xid, 0).unwrap();
                    tables.idx_order_line.insert(
                        make_key(&[
                            &i32_key(w_id as i32),
                            &i32_key(d_id as i32),
                            &i32_key(o_id as i32),
                            &i32_key(ol_number as i32),
                        ]),
                        tid,
                    );
                }
            }
        }

        if w_id % 1 == 0 || w_id == num_warehouses {
            println!("  Warehouse {} loaded", w_id);
        }
    }
}

// ---------------------------------------------------------------------------
// TPCC Statistics
// ---------------------------------------------------------------------------

/// Per-transaction-type statistics (matching C++ TransactionRunStat)
#[derive(Debug)]
struct TxnStats {
    committed: AtomicU64,
    aborted: AtomicU64,
}

impl TxnStats {
    fn new() -> Self {
        Self {
            committed: AtomicU64::new(0),
            aborted: AtomicU64::new(0),
        }
    }
}

const TXN_NEW_ORDER: usize = 0;
const TXN_PAYMENT: usize = 1;
const TXN_ORDER_STATUS: usize = 2;
const TXN_DELIVERY: usize = 3;
const TXN_STOCK_LEVEL: usize = 4;
const TXN_COUNT: usize = 5;

const TXN_NAMES: [&str; 5] = ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"];

struct Stats {
    per_txn: [TxnStats; TXN_COUNT],
}

impl Stats {
    fn new() -> Self {
        Self {
            per_txn: [
                TxnStats::new(),
                TxnStats::new(),
                TxnStats::new(),
                TxnStats::new(),
                TxnStats::new(),
            ],
        }
    }

    fn commit(&self, txn_type: usize) {
        self.per_txn[txn_type].committed.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn abort(&self, txn_type: usize) {
        self.per_txn[txn_type].aborted.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn total_committed(&self) -> u64 {
        self.per_txn.iter().map(|s| s.committed.load(AtomicOrdering::Relaxed)).sum()
    }

    fn total_aborted(&self) -> u64 {
        self.per_txn.iter().map(|s| s.aborted.load(AtomicOrdering::Relaxed)).sum()
    }

}

// ---------------------------------------------------------------------------
// TPCC Transactions (matching C++ logic exactly)
// ---------------------------------------------------------------------------

/// New-Order transaction (45% weight).
///
/// Matching C++ NewOrderTransaction():
/// 1. SELECT district FOR UPDATE -> d_next_o_id, d_tax
/// 2. SELECT warehouse -> w_tax
/// 3. SELECT customer -> c_discount
/// 4. UPDATE district SET d_next_o_id += 1
/// 5. INSERT ORDER
/// 6. INSERT NEW_ORDER
/// 7. For each order line:
///    a. SELECT item
///    b. SELECT stock FOR UPDATE
///    c. UPDATE stock
///    d. INSERT ORDER_LINE
fn execute_new_order<R: Rng>(
    tables: &TpccTables,
    txn_mgr: &Arc<TransactionManager>,
    rng: &mut R,
    num_warehouses: u32,
    item_num: u32,
) -> ferrisdb_core::Result<bool> {
    let mut txn = txn_mgr.begin()?;
    let xid = txn.xid();

    let w_id = random_number(rng, 1, num_warehouses as i32);
    let d_id = random_number(rng, 1, DISTRICTS_PER_WAREHOUSE as i32);
    let c_id = nu_rand(rng, 1023, 1, tables.customer_per_district as i32);
    let ol_cnt = random_number(rng, 5, MAX_NUM_ITEMS as i32);
    let rbk = random_number(rng, 1, 100);

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Prepare order line arguments
    let mut ol_i_ids: Vec<i32> = Vec::with_capacity(ol_cnt as usize);
    let mut ol_supply_w_ids: Vec<i32> = Vec::with_capacity(ol_cnt as usize);
    let mut ol_quantities: Vec<i32> = Vec::with_capacity(ol_cnt as usize);
    let mut all_local: i32 = 1;

    for i in 0..ol_cnt {
        let mut ol_i_id = nu_rand(rng, 8191, 1, item_num as i32);
        if i == ol_cnt - 1 && rbk == 1 {
            ol_i_id = item_num as i32 + 1; // simulate user error -> rollback
        }

        let ol_supply_w_id = if random_number(rng, 1, 100) != 0 {
            w_id
        } else {
            all_local = 0;
            get_other_warehouse_id(rng, w_id, num_warehouses as i32)
        };
        let ol_quantity = random_number(rng, 1, 10);

        ol_i_ids.push(ol_i_id);
        ol_supply_w_ids.push(ol_supply_w_id);
        ol_quantities.push(ol_quantity);
    }

    // Sort order lines by (ol_supply_w_id, ol_i_id) to avoid deadlocks
    // (matching C++ sort logic)
    let mut indices: Vec<usize> = (0..ol_i_ids.len()).collect();
    indices.sort_by(|&a, &b| {
        (ol_supply_w_ids[a], ol_i_ids[a]).cmp(&(ol_supply_w_ids[b], ol_i_ids[b]))
    });

    // Step 1: SELECT district FOR UPDATE
    let d_key = make_key(&[&i32_key(w_id), &i32_key(d_id)]);
    let d_tid = match tables.idx_district.get(&d_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_d_hdr, d_data) = match tables.district.fetch(d_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let mut d_reader = TupleReader::new(&d_data);
    let _d_id_val = d_reader.read_int32().unwrap_or(0);  // D_ID
    let _d_w_id = d_reader.read_int32().unwrap_or(0);    // D_W_ID
    let d_ytd = d_reader.read_float64().unwrap_or(0.0);  // D_YTD
    let d_tax = d_reader.read_float64().unwrap_or(0.0);  // D_TAX
    let d_next_o_id = d_reader.read_int32().unwrap_or(1); // D_NEXT_O_ID
    let o_id = d_next_o_id;

    // Step 2: SELECT warehouse
    let w_key = make_key(&[&i32_key(w_id)]);
    let w_tid = match tables.idx_warehouse.get(&w_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_w_hdr, w_data) = match tables.warehouse.fetch(w_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let mut w_reader = TupleReader::new(&w_data);
    let _w_id_val = w_reader.read_int32().unwrap_or(0); // W_ID
    let _w_ytd = w_reader.read_float64().unwrap_or(0.0);
    let w_tax = w_reader.read_float64().unwrap_or(0.0);  // W_TAX

    // Step 3: SELECT customer
    let c_key = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(c_id)]);
    let c_tid = match tables.idx_customer.get(&c_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_c_hdr, c_data) = match tables.customer.fetch(c_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let mut c_reader = TupleReader::new(&c_data);
    c_reader.read_int32(); // C_W_ID
    c_reader.read_int32(); // C_D_ID
    c_reader.read_int32(); // C_ID
    let c_discount = c_reader.read_float64().unwrap_or(0.0); // C_DISCOUNT

    // Step 4: UPDATE district SET d_next_o_id += 1
    {
        let mut new_d = TupleBuilder::with_capacity(d_data.len());
        new_d.put_int32(d_id);                          // D_ID
        new_d.put_int32(w_id);                          // D_W_ID
        new_d.put_float64(d_ytd);                       // D_YTD (unchanged)
        new_d.put_float64(d_tax);                       // D_TAX (unchanged)
        new_d.put_int32(d_next_o_id + 1);               // D_NEXT_O_ID += 1
        // Copy remaining fields from old data
        // Skip the first 5 fields we already read
        let mut copy_reader = TupleReader::new(&d_data);
        for _ in 0..5 { copy_reader.skip(); }
        // D_NAME
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }
        // D_STREET_1
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }
        // D_STREET_2
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }
        // D_CITY
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }
        // D_STATE
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }
        // D_ZIP
        if let Some(s) = copy_reader.read_string() { new_d.put_string(&s); } else { new_d.put_string(""); }

        let new_data = new_d.build();
        tables.district.update(d_tid, &new_data, xid, 0)?;
    }

    // Step 5: INSERT ORDER
    {
        let mut ob = TupleBuilder::with_capacity(128);
        ob.put_int32(o_id);           // O_ID
        ob.put_int32(d_id);           // O_D_ID
        ob.put_int32(w_id);           // O_W_ID
        ob.put_int32(c_id);           // O_C_ID
        ob.put_int32(0);              // O_CARRIER_ID (null for new order)
        ob.put_int32(ol_cnt);         // O_OL_CNT
        ob.put_int32(all_local);      // O_ALL_LOCAL
        ob.put_timestamp(now_ts);     // O_ENTRY_D

        let data = ob.build();
        let tid = tables.order.insert(&data, xid, 0)?;
        tables.idx_order.insert(
            make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(o_id)]),
            tid,
        );
        // Also index by customer for ORDER_STATUS
        tables.idx_order_cust.insert(
            make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(c_id)]),
            tid,
        );
    }

    // Step 6: INSERT NEW_ORDER
    {
        let mut nb = TupleBuilder::with_capacity(64);
        nb.put_int32(o_id);           // NO_O_ID
        nb.put_int32(d_id);           // NO_D_ID
        nb.put_int32(w_id);           // NO_W_ID

        let data = nb.build();
        let tid = tables.new_order.insert(&data, xid, 0)?;
        tables.idx_new_order.insert(
            make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(o_id)]),
            tid,
        );
    }

    // Step 7: Process order lines
    let mut _total_amount: f64 = 0.0;
    for &idx in &indices {
        let ol_i_id = ol_i_ids[idx];
        let ol_supply_w_id = ol_supply_w_ids[idx];
        let ol_quantity = ol_quantities[idx];
        let ol_number = (idx + 1) as i32;

        // 7a: SELECT item
        let i_key = make_key(&[&i32_key(ol_i_id)]);
        let i_tid = match tables.idx_item.get(&i_key) {
            Some(tid) => tid,
            None => {
                // Item not found -> rollback (simulating user error)
                if ol_i_id > item_num as i32 && rbk == 1 {
                    let _ = txn.abort();
                    return Ok(false);
                }
                let _ = txn.abort();
                return Ok(false);
            }
        };
        let (_i_hdr, i_data) = match tables.item.fetch(i_tid)? {
            Some(pair) => pair,
            None => {
                let _ = txn.abort();
                return Ok(false);
            }
        };
        let mut i_reader = TupleReader::new(&i_data);
        let _i_id_val = i_reader.read_int32().unwrap_or(0); // I_ID
        let _i_name = i_reader.read_string().unwrap_or_default();
        let i_price = i_reader.read_float64().unwrap_or(0.0); // I_PRICE

        // 7b: SELECT stock FOR UPDATE (by ol_supply_w_id, ol_i_id)
        let s_key = make_key(&[&i32_key(ol_supply_w_id), &i32_key(ol_i_id)]);
        let s_tid = match tables.idx_stock.get(&s_key) {
            Some(tid) => tid,
            None => {
                let _ = txn.abort();
                return Ok(false);
            }
        };
        let (_s_hdr, s_data) = match tables.stock.fetch(s_tid)? {
            Some(pair) => pair,
            None => {
                let _ = txn.abort();
                return Ok(false);
            }
        };
        let mut s_reader = TupleReader::new(&s_data);
        let _s_w_id = s_reader.read_int32().unwrap_or(0); // S_W_ID
        let _s_i_id = s_reader.read_int32().unwrap_or(0); // S_I_ID
        let s_quantity = s_reader.read_int32().unwrap_or(0); // S_QUANTITY
        let s_ytd = s_reader.read_int32().unwrap_or(0);     // S_YTD
        let s_order_cnt = s_reader.read_int32().unwrap_or(0); // S_ORDER_CNT
        let s_remote_cnt = s_reader.read_int32().unwrap_or(0); // S_REMOTE_CNT

        // Compute updated values
        let ol_amount = i_price * ol_quantity as f64;
        _total_amount += ol_amount * (1.0 - c_discount) * (1.0 + w_tax + d_tax);

        let remote_cnt = if ol_supply_w_id == w_id { 0 } else { 1 };
        let new_s_quantity = if s_quantity >= ol_quantity + 10 {
            s_quantity - ol_quantity
        } else {
            s_quantity + 91
        };

        // 7c: UPDATE stock
        {
            let mut new_s = TupleBuilder::with_capacity(s_data.len());
            new_s.put_int32(ol_supply_w_id);                // S_W_ID
            new_s.put_int32(ol_i_id);                       // S_I_ID
            new_s.put_int32(new_s_quantity);                 // S_QUANTITY
            new_s.put_int32(s_ytd + ol_quantity);            // S_YTD
            new_s.put_int32(s_order_cnt + 1);                // S_ORDER_CNT
            new_s.put_int32(s_remote_cnt + remote_cnt);      // S_REMOTE_CNT
            // Copy remaining fields from old stock data
            let mut copy_reader = TupleReader::new(&s_data);
            for _ in 0..6 { copy_reader.skip(); }
            // S_DATA
            if let Some(s) = copy_reader.read_string() { new_s.put_string(&s); } else { new_s.put_string(""); }
            // S_DIST_01..S_DIST_10
            for _ in 0..10 {
                if let Some(s) = copy_reader.read_string() { new_s.put_string(&s); } else { new_s.put_string(""); }
            }

            let new_data = new_s.build();
            tables.stock.update(s_tid, &new_data, xid, 0)?;
        }

        // 7d: INSERT ORDER_LINE
        {
            let mut olb = TupleBuilder::with_capacity(128);
            olb.put_int32(w_id);              // OL_W_ID
            olb.put_int32(d_id);              // OL_D_ID
            olb.put_int32(o_id);              // OL_O_ID
            olb.put_int32(ol_number);         // OL_NUMBER
            olb.put_int32(ol_i_id);           // OL_I_ID
            olb.put_int32(ol_supply_w_id);    // OL_SUPPLY_W_ID
            olb.put_timestamp(0);             // OL_DELIVERY_D (null)
            olb.put_int32(ol_quantity);       // OL_QUANTITY
            olb.put_float64(ol_amount);       // OL_AMOUNT
            olb.put_string(&format!("s_dist_{:02}", d_id)); // OL_DIST_INFO

            let data = olb.build();
            let tid = tables.order_line.insert(&data, xid, 0)?;
            tables.idx_order_line.insert(
                make_key(&[
                    &i32_key(w_id),
                    &i32_key(d_id),
                    &i32_key(o_id),
                    &i32_key(ol_number),
                ]),
                tid,
            );
        }
    }

    txn.commit()?;

    Ok(true)
}

/// Payment transaction (43% weight).
///
/// Matching C++ PaymentTransaction():
/// 1. UPDATE district SET d_ytd += h_amount
/// 2. SELECT district
/// 3. UPDATE warehouse SET w_ytd += h_amount
/// 4. SELECT warehouse
/// 5. SELECT customer
/// 6. UPDATE customer
/// 7. INSERT history
fn execute_payment<R: Rng>(
    tables: &TpccTables,
    txn_mgr: &Arc<TransactionManager>,
    rng: &mut R,
    num_warehouses: u32,
) -> ferrisdb_core::Result<bool> {
    let mut txn = txn_mgr.begin()?;
    let xid = txn.xid();

    let w_id = random_number(rng, 1, num_warehouses as i32);
    let d_id = random_number(rng, 1, DISTRICTS_PER_WAREHOUSE as i32);
    let c_id = nu_rand(rng, 1023, 1, tables.customer_per_district as i32);
    let h_amount = random_number(rng, 1, 5000) as f64;

    // 85% same warehouse, 15% remote
    let (c_w_id, c_d_id) = if random_number(rng, 1, 100) <= 85 {
        (w_id, d_id)
    } else {
        (
            get_other_warehouse_id(rng, w_id, num_warehouses as i32),
            random_number(rng, 1, DISTRICTS_PER_WAREHOUSE as i32),
        )
    };

    // Step 1: UPDATE district d_ytd += h_amount
    let d_key = make_key(&[&i32_key(w_id), &i32_key(d_id)]);
    let d_tid = match tables.idx_district.get(&d_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_d_hdr, d_data) = match tables.district.fetch(d_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    // Parse all district fields and update d_ytd
    let mut d_reader = TupleReader::new(&d_data);
    let d_id_v = d_reader.read_int32().unwrap_or(0);
    let d_w_id_v = d_reader.read_int32().unwrap_or(0);
    let d_ytd = d_reader.read_float64().unwrap_or(0.0);
    let d_tax = d_reader.read_float64().unwrap_or(0.0);
    let d_next_o_id = d_reader.read_int32().unwrap_or(1);
    let d_name = d_reader.read_string().unwrap_or_default();
    let d_st1 = d_reader.read_string().unwrap_or_default();
    let d_st2 = d_reader.read_string().unwrap_or_default();
    let d_city = d_reader.read_string().unwrap_or_default();
    let d_state = d_reader.read_string().unwrap_or_default();
    let d_zip = d_reader.read_string().unwrap_or_default();

    {
        let mut new_d = TupleBuilder::with_capacity(d_data.len());
        new_d.put_int32(d_id_v);
        new_d.put_int32(d_w_id_v);
        new_d.put_float64(d_ytd + h_amount); // d_ytd += h_amount
        new_d.put_float64(d_tax);
        new_d.put_int32(d_next_o_id);
        new_d.put_string(&d_name);
        new_d.put_string(&d_st1);
        new_d.put_string(&d_st2);
        new_d.put_string(&d_city);
        new_d.put_string(&d_state);
        new_d.put_string(&d_zip);
        let new_data = new_d.build();
        tables.district.update(d_tid, &new_data, xid, 0)?;
    }

    // Step 3: UPDATE warehouse w_ytd += h_amount
    let w_key = make_key(&[&i32_key(w_id)]);
    let w_tid = match tables.idx_warehouse.get(&w_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_w_hdr, w_data) = match tables.warehouse.fetch(w_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let mut w_reader = TupleReader::new(&w_data);
    let w_id_v = w_reader.read_int32().unwrap_or(0);
    let w_ytd = w_reader.read_float64().unwrap_or(0.0);
    let w_tax = w_reader.read_float64().unwrap_or(0.0);
    let w_name = w_reader.read_string().unwrap_or_default();
    let w_st1 = w_reader.read_string().unwrap_or_default();
    let w_st2 = w_reader.read_string().unwrap_or_default();
    let w_city = w_reader.read_string().unwrap_or_default();
    let w_state = w_reader.read_string().unwrap_or_default();
    let w_zip = w_reader.read_string().unwrap_or_default();

    {
        let mut new_w = TupleBuilder::with_capacity(w_data.len());
        new_w.put_int32(w_id_v);
        new_w.put_float64(w_ytd + h_amount); // w_ytd += h_amount
        new_w.put_float64(w_tax);
        new_w.put_string(&w_name);
        new_w.put_string(&w_st1);
        new_w.put_string(&w_st2);
        new_w.put_string(&w_city);
        new_w.put_string(&w_state);
        new_w.put_string(&w_zip);
        let new_data = new_w.build();
        tables.warehouse.update(w_tid, &new_data, xid, 0)?;
    }

    // Step 5: SELECT customer
    let c_key = make_key(&[&i32_key(c_w_id), &i32_key(c_d_id), &i32_key(c_id)]);
    let c_tid = match tables.idx_customer.get(&c_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let (_c_hdr, c_data) = match tables.customer.fetch(c_tid)? {
        Some(pair) => pair,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };

    // Step 6: UPDATE customer (c_balance -= h_amount, c_ytd_payment += h_amount, c_payment_cnt += 1)
    {
        let mut c_reader = TupleReader::new(&c_data);
        let c_w_id_v = c_reader.read_int32().unwrap_or(0);
        let c_d_id_v = c_reader.read_int32().unwrap_or(0);
        let c_id_v = c_reader.read_int32().unwrap_or(0);
        let c_discount = c_reader.read_float64().unwrap_or(0.0);
        let c_credit = c_reader.read_string().unwrap_or_default();
        let c_last = c_reader.read_string().unwrap_or_default();
        let c_first = c_reader.read_string().unwrap_or_default();
        let c_credit_lim = c_reader.read_float64().unwrap_or(50000.0);
        let c_balance = c_reader.read_float64().unwrap_or(0.0);
        let c_ytd_payment = c_reader.read_float64().unwrap_or(0.0);
        let c_payment_cnt = c_reader.read_int32().unwrap_or(0);
        let c_delivery_cnt = c_reader.read_int32().unwrap_or(0);
        let c_st1 = c_reader.read_string().unwrap_or_default();
        let c_st2 = c_reader.read_string().unwrap_or_default();
        let c_city = c_reader.read_string().unwrap_or_default();
        let c_state = c_reader.read_string().unwrap_or_default();
        let c_zip = c_reader.read_string().unwrap_or_default();
        let c_phone = c_reader.read_string().unwrap_or_default();
        let c_since = c_reader.read_timestamp().unwrap_or(0);
        let c_middle = c_reader.read_string().unwrap_or_default();
        let c_data_val = c_reader.read_string().unwrap_or_default();

        let mut new_c = TupleBuilder::with_capacity(c_data.len());
        new_c.put_int32(c_w_id_v);
        new_c.put_int32(c_d_id_v);
        new_c.put_int32(c_id_v);
        new_c.put_float64(c_discount);
        new_c.put_string(&c_credit);
        new_c.put_string(&c_last);
        new_c.put_string(&c_first);
        new_c.put_float64(c_credit_lim);
        new_c.put_float64(c_balance - h_amount);          // c_balance -= h_amount
        new_c.put_float64(c_ytd_payment + h_amount);      // c_ytd_payment += h_amount
        new_c.put_int32(c_payment_cnt + 1);               // c_payment_cnt += 1
        new_c.put_int32(c_delivery_cnt);
        new_c.put_string(&c_st1);
        new_c.put_string(&c_st2);
        new_c.put_string(&c_city);
        new_c.put_string(&c_state);
        new_c.put_string(&c_zip);
        new_c.put_string(&c_phone);
        new_c.put_timestamp(c_since);
        new_c.put_string(&c_middle);
        new_c.put_string(&c_data_val);

        let new_data = new_c.build();
        tables.customer.update(c_tid, &new_data, xid, 0)?;
    }

    // Step 7: INSERT history
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    {
        let mut hb = TupleBuilder::with_capacity(128);
        hb.put_int32(0);              // H_ID (auto)
        hb.put_int32(c_id);           // H_C_ID
        hb.put_int32(c_d_id);         // H_C_D_ID
        hb.put_int32(c_w_id);         // H_C_W_ID
        hb.put_int32(d_id);           // H_D_ID
        hb.put_int32(w_id);           // H_W_ID
        hb.put_timestamp(now_ts);     // H_DATE
        hb.put_float64(h_amount);     // H_AMOUNT
        hb.put_string("mon");         // H_DATA

        let data = hb.build();
        tables.history.insert(&data, xid, 0)?;
    }

    txn.commit()?;
    Ok(true)
}

/// Order-Status transaction (4% weight).
///
/// Matching C++ OrderStatusTransaction():
/// 1. SELECT customer
/// 2. SELECT last order by c_id -> get o_id
/// 3. SELECT order_line by o_id
fn execute_order_status<R: Rng>(
    tables: &TpccTables,
    txn_mgr: &Arc<TransactionManager>,
    rng: &mut R,
    num_warehouses: u32,
) -> ferrisdb_core::Result<bool> {
    let mut txn = txn_mgr.begin()?;

    let w_id = random_number(rng, 1, num_warehouses as i32);
    let d_id = random_number(rng, 1, DISTRICTS_PER_WAREHOUSE as i32);
    let c_id = nu_rand(rng, 1023, 1, tables.customer_per_district as i32);

    // Step 1: SELECT customer
    let c_key = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(c_id)]);
    let c_tid = match tables.idx_customer.get(&c_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    if tables.customer.fetch(c_tid)?.is_none() {
        let _ = txn.abort();
        return Ok(false);
    }

    // Step 2: Find the latest order for this customer using customer-order index
    let oc_key = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(c_id)]);
    let latest_tid = match tables.idx_order_cust.get(&oc_key) {
        Some(tid) => tid,
        None => {
            txn.commit()?;
            return Ok(true);
        }
    };

    let latest_o_id = if let Some((_hdr, data)) = tables.order.fetch(latest_tid)? {
        let mut reader = TupleReader::new(&data);
        reader.read_int32().unwrap_or(-1)
    } else {
        txn.commit()?;
        return Ok(true);
    };

    if latest_o_id < 0 {
        // No orders found for this customer
        txn.commit()?;
        return Ok(true);
    }

    // Step 3: SELECT order_line for this order
    let ol_prefix = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(latest_o_id)]);
    let ol_entries = tables.idx_order_line.scan_prefix(&ol_prefix);
    for (_key, tid) in &ol_entries {
        // Read each order line (exercise the read path)
        let _ = tables.order_line.fetch(*tid)?;
    }

    txn.commit()?;
    Ok(true)
}

/// Delivery transaction (4% weight).
///
/// Matching C++ DeliveryTransaction():
/// For each district:
/// 1. SELECT MIN(o_id) FROM NEW_ORDER
/// 2. DELETE from NEW_ORDER
/// 3. UPDATE ORDER SET o_carrier_id
/// 4. UPDATE ORDER_LINE SET ol_delivery_d
/// 5. UPDATE customer SET c_delivery_cnt += 1, c_balance += ol_amount_sum
fn execute_delivery<R: Rng>(
    tables: &TpccTables,
    txn_mgr: &Arc<TransactionManager>,
    rng: &mut R,
    num_warehouses: u32,
) -> ferrisdb_core::Result<bool> {
    let mut txn = txn_mgr.begin()?;
    let xid = txn.xid();

    let w_id = random_number(rng, 1, num_warehouses as i32);
    let o_carrier_id = random_number(rng, 1, 10);

    for d_id in 1..=DISTRICTS_PER_WAREHOUSE as i32 {
        // Step 1: SELECT MIN(o_id) from NEW_ORDER matching (w_id, d_id)
        let no_prefix = make_key(&[&i32_key(w_id), &i32_key(d_id)]);
        let (no_key, no_tid) = match tables.idx_new_order.find_min_with_prefix(&no_prefix) {
            Some(pair) => pair,
            None => continue, // no new orders for this district
        };

        // Get the o_id from the new_order tuple
        let (_no_hdr, no_data) = match tables.new_order.fetch(no_tid)? {
            Some(pair) => pair,
            None => continue,
        };
        let mut no_reader = TupleReader::new(&no_data);
        let no_o_id = no_reader.read_int32().unwrap_or(0);

        // Step 2: DELETE from NEW_ORDER
        tables.new_order.delete(no_tid, xid, 0)?;
        tables.idx_new_order.remove(&no_key);

        // Step 3: UPDATE ORDER SET o_carrier_id
        let o_key = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(no_o_id)]);
        let o_tid = match tables.idx_order.get(&o_key) {
            Some(tid) => tid,
            None => continue,
        };
        let (_o_hdr, o_data) = match tables.order.fetch(o_tid)? {
            Some(pair) => pair,
            None => continue,
        };
        let mut o_reader = TupleReader::new(&o_data);
        let o_id_val = o_reader.read_int32().unwrap_or(0);
        let o_d_id = o_reader.read_int32().unwrap_or(0);
        let o_w_id = o_reader.read_int32().unwrap_or(0);
        let o_c_id = o_reader.read_int32().unwrap_or(0);
        let _old_carrier = o_reader.read_int32().unwrap_or(0);
        let o_ol_cnt = o_reader.read_int32().unwrap_or(0);
        let o_all_local = o_reader.read_int32().unwrap_or(0);
        let o_entry_d = o_reader.read_timestamp().unwrap_or(0);

        {
            let mut new_o = TupleBuilder::with_capacity(o_data.len());
            new_o.put_int32(o_id_val);
            new_o.put_int32(o_d_id);
            new_o.put_int32(o_w_id);
            new_o.put_int32(o_c_id);
            new_o.put_int32(o_carrier_id); // updated carrier_id
            new_o.put_int32(o_ol_cnt);
            new_o.put_int32(o_all_local);
            new_o.put_timestamp(o_entry_d);
            let new_data = new_o.build();
            tables.order.update(o_tid, &new_data, xid, 0)?;
        }

        // Step 4: UPDATE ORDER_LINE SET ol_delivery_d
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let ol_prefix = make_key(&[&i32_key(w_id), &i32_key(d_id), &i32_key(no_o_id)]);
        let mut ol_amount_sum: f64 = 0.0;

        // Use point lookups instead of prefix scan
        for ol_number in 1..=MAX_NUM_ITEMS as i32 {
            let ol_key = make_key(&[
                &i32_key(w_id),
                &i32_key(d_id),
                &i32_key(no_o_id),
                &i32_key(ol_number),
            ]);
            let ol_tid = match tables.idx_order_line.get(&ol_key) {
                Some(tid) => tid,
                None => continue,
            };
            if let Some((_ol_hdr, ol_data)) = tables.order_line.fetch(ol_tid)? {
                let mut ol_reader = TupleReader::new(&ol_data);
                let ol_w_id = ol_reader.read_int32().unwrap_or(0);
                let ol_d_id = ol_reader.read_int32().unwrap_or(0);
                let ol_o_id = ol_reader.read_int32().unwrap_or(0);
                let ol_number = ol_reader.read_int32().unwrap_or(0);
                let ol_i_id = ol_reader.read_int32().unwrap_or(0);
                let ol_supply_w_id = ol_reader.read_int32().unwrap_or(0);
                let _ol_delivery_d = ol_reader.read_timestamp().unwrap_or(0);
                let ol_quantity = ol_reader.read_int32().unwrap_or(0);
                let ol_amount = ol_reader.read_float64().unwrap_or(0.0);
                let ol_dist_info = ol_reader.read_string().unwrap_or_default();

                ol_amount_sum += ol_amount;

                let mut new_ol = TupleBuilder::with_capacity(ol_data.len());
                new_ol.put_int32(ol_w_id);
                new_ol.put_int32(ol_d_id);
                new_ol.put_int32(ol_o_id);
                new_ol.put_int32(ol_number);
                new_ol.put_int32(ol_i_id);
                new_ol.put_int32(ol_supply_w_id);
                new_ol.put_timestamp(now_ts); // updated delivery_d
                new_ol.put_int32(ol_quantity);
                new_ol.put_float64(ol_amount);
                new_ol.put_string(&ol_dist_info);
                let new_data = new_ol.build();
                tables.order_line.update(ol_tid, &new_data, xid, 0)?;
            }
        }

        // Step 5: UPDATE customer SET c_delivery_cnt += 1, c_balance += ol_amount_sum
        let c_key = make_key(&[&i32_key(o_w_id), &i32_key(o_d_id), &i32_key(o_c_id)]);
        let c_tid = match tables.idx_customer.get(&c_key) {
            Some(tid) => tid,
            None => continue,
        };
        if let Some((_c_hdr, c_data)) = tables.customer.fetch(c_tid)? {
            let mut c_reader = TupleReader::new(&c_data);
            let c_w_id_v = c_reader.read_int32().unwrap_or(0);
            let c_d_id_v = c_reader.read_int32().unwrap_or(0);
            let c_id_v = c_reader.read_int32().unwrap_or(0);
            let c_discount = c_reader.read_float64().unwrap_or(0.0);
            let c_credit = c_reader.read_string().unwrap_or_default();
            let c_last = c_reader.read_string().unwrap_or_default();
            let c_first = c_reader.read_string().unwrap_or_default();
            let c_credit_lim = c_reader.read_float64().unwrap_or(50000.0);
            let c_balance = c_reader.read_float64().unwrap_or(0.0);
            let c_ytd_payment = c_reader.read_float64().unwrap_or(0.0);
            let c_payment_cnt = c_reader.read_int32().unwrap_or(0);
            let c_delivery_cnt = c_reader.read_int32().unwrap_or(0);
            let c_st1 = c_reader.read_string().unwrap_or_default();
            let c_st2 = c_reader.read_string().unwrap_or_default();
            let c_city = c_reader.read_string().unwrap_or_default();
            let c_state = c_reader.read_string().unwrap_or_default();
            let c_zip = c_reader.read_string().unwrap_or_default();
            let c_phone = c_reader.read_string().unwrap_or_default();
            let c_since = c_reader.read_timestamp().unwrap_or(0);
            let c_middle = c_reader.read_string().unwrap_or_default();
            let c_data_val = c_reader.read_string().unwrap_or_default();

            let mut new_c = TupleBuilder::with_capacity(c_data.len());
            new_c.put_int32(c_w_id_v);
            new_c.put_int32(c_d_id_v);
            new_c.put_int32(c_id_v);
            new_c.put_float64(c_discount);
            new_c.put_string(&c_credit);
            new_c.put_string(&c_last);
            new_c.put_string(&c_first);
            new_c.put_float64(c_credit_lim);
            new_c.put_float64(c_balance + ol_amount_sum); // c_balance += ol_amount_sum
            new_c.put_float64(c_ytd_payment);
            new_c.put_int32(c_payment_cnt);
            new_c.put_int32(c_delivery_cnt + 1);          // c_delivery_cnt += 1
            new_c.put_string(&c_st1);
            new_c.put_string(&c_st2);
            new_c.put_string(&c_city);
            new_c.put_string(&c_state);
            new_c.put_string(&c_zip);
            new_c.put_string(&c_phone);
            new_c.put_timestamp(c_since);
            new_c.put_string(&c_middle);
            new_c.put_string(&c_data_val);

            let new_data = new_c.build();
            tables.customer.update(c_tid, &new_data, xid, 0)?;
        }
    }

    txn.commit()?;
    Ok(true)
}

/// Stock-Level transaction (4% weight).
///
/// Matching C++ StockLevelTransaction():
/// 1. SELECT district -> d_next_o_id
/// 2. Scan order_line for range [d_next_o_id-20, d_next_o_id-1]
/// 3. Count distinct items with s_quantity < threshold
fn execute_stock_level<R: Rng>(
    tables: &TpccTables,
    txn_mgr: &Arc<TransactionManager>,
    rng: &mut R,
    num_warehouses: u32,
) -> ferrisdb_core::Result<bool> {
    let mut txn = txn_mgr.begin()?;

    let w_id = random_number(rng, 1, num_warehouses as i32);
    let d_id = random_number(rng, 1, DISTRICTS_PER_WAREHOUSE as i32);
    let threshold = random_number(rng, 10, 20);

    // Step 1: SELECT district -> d_next_o_id
    let d_key = make_key(&[&i32_key(w_id), &i32_key(d_id)]);
    let d_tid = match tables.idx_district.get(&d_key) {
        Some(tid) => tid,
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };
    let d_next_o_id = match tables.district.fetch(d_tid)? {
        Some((_hdr, data)) => {
            let mut reader = TupleReader::new(&data);
            reader.read_int32(); // D_ID
            reader.read_int32(); // D_W_ID
            reader.read_float64(); // D_YTD
            reader.read_float64(); // D_TAX
            reader.read_int32().unwrap_or(1) // D_NEXT_O_ID
        }
        None => {
            let _ = txn.abort();
            return Ok(false);
        }
    };

    // Step 2: Scan order lines for orders in [d_next_o_id-20, d_next_o_id-1]
    // Use point lookups instead of prefix scan (each order has at most 15 lines)
    let range_start = d_next_o_id - 20;
    let range_end = d_next_o_id - 1;

    let mut item_ids = std::collections::HashSet::new();
    for o_id in range_start..=range_end {
        for ol_number in 1..=MAX_NUM_ITEMS as i32 {
            let ol_key = make_key(&[
                &i32_key(w_id),
                &i32_key(d_id),
                &i32_key(o_id),
                &i32_key(ol_number),
            ]);
            if let Some(ol_tid) = tables.idx_order_line.get(&ol_key) {
                if let Some((_hdr, ol_data)) = tables.order_line.fetch(ol_tid)? {
                    let mut reader = TupleReader::new(&ol_data);
                    reader.read_int32(); // OL_W_ID
                    reader.read_int32(); // OL_D_ID
                    reader.read_int32(); // OL_O_ID
                    reader.read_int32(); // OL_NUMBER
                    let ol_i_id = reader.read_int32().unwrap_or(0);
                    if ol_i_id > 0 {
                        item_ids.insert(ol_i_id);
                    }
                }
            }
        }
    }

    // Step 3: Count items with s_quantity < threshold
    let mut low_stock: i32 = 0;
    for &i_id in &item_ids {
        let s_key = make_key(&[&i32_key(w_id), &i32_key(i_id)]);
        if let Some(s_tid) = tables.idx_stock.get(&s_key) {
            if let Some((_hdr, s_data)) = tables.stock.fetch(s_tid)? {
                let mut reader = TupleReader::new(&s_data);
                reader.read_int32(); // S_W_ID
                reader.read_int32(); // S_I_ID
                let s_quantity = reader.read_int32().unwrap_or(0);
                if s_quantity < threshold {
                    low_stock += 1;
                }
            }
        }
    }

    let _ = low_stock; // result of the query

    txn.commit()?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Worker Thread
// ---------------------------------------------------------------------------

fn worker(
    thread_id: usize,
    tables: Arc<TpccTables>,
    txn_mgr: Arc<TransactionManager>,
    stats: Arc<Stats>,
    num_warehouses: u32,
    item_num: u32,
    duration: Duration,
) {
    let mut rng = StdRng::seed_from_u64(thread_id as u64 * 7919 + 12345);
    let start = Instant::now();

    while start.elapsed() < duration {
        // Match C++ transaction mix: 45/43/4/4/4
        let r: f64 = rng.gen();
        let (txn_type, result) = if r <= 0.45 {
            (TXN_NEW_ORDER, execute_new_order(&tables, &txn_mgr, &mut rng, num_warehouses, item_num))
        } else if r <= 0.88 {
            (TXN_PAYMENT, execute_payment(&tables, &txn_mgr, &mut rng, num_warehouses))
        } else if r <= 0.92 {
            (TXN_ORDER_STATUS, execute_order_status(&tables, &txn_mgr, &mut rng, num_warehouses))
        } else if r <= 0.96 {
            (TXN_DELIVERY, execute_delivery(&tables, &txn_mgr, &mut rng, num_warehouses))
        } else {
            (TXN_STOCK_LEVEL, execute_stock_level(&tables, &txn_mgr, &mut rng, num_warehouses))
        };

        match result {
            Ok(true) => stats.commit(txn_type),
            Ok(false) => stats.abort(txn_type),
            Err(_) => stats.abort(txn_type),
        }
    }
}

// ---------------------------------------------------------------------------
// CLI Arguments & main()
// ---------------------------------------------------------------------------

/// TPCC Benchmark Arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "FerrisDB TPC-C Benchmark", long_about = None)]
struct Args {
    /// Number of warehouses
    #[arg(short, long, default_value = "1")]
    warehouses: u32,

    /// Number of threads
    #[arg(short, long, default_value = "20")]
    threads: usize,

    /// Duration in seconds
    #[arg(short, long, default_value = "120")]
    duration: u64,

    /// Number of items per warehouse
    #[arg(long, default_value = "10000")]
    items: u32,

    /// Number of customers per district
    #[arg(long, default_value = "300")]
    customers: u32,

    /// Number of orders per district
    #[arg(long, default_value = "300")]
    orders: u32,

    /// Buffer pool size (number of pages)
    #[arg(long, default_value = "500000")]
    buffer_size: usize,

    /// Data directory (optional, uses temp dir if not specified)
    #[arg(long)]
    data_dir: Option<String>,

    /// Enable WAL persistence to disk
    #[arg(long, default_value = "false")]
    wal: bool,
}

fn main() {
    let args = Args::parse();

    println!("FerrisDB TPC-C Benchmark");
    println!("======================");
    println!("Warehouses:            {}", args.warehouses);
    println!("Threads:               {}", args.threads);
    println!("Duration:              {}s", args.duration);
    println!("Items/warehouse:       {}", args.items);
    println!("Customers/district:    {}", args.customers);
    println!("Orders/district:       {}", args.orders);
    println!("Buffer Size:           {} pages", args.buffer_size);
    println!();

    // Create data directory
    let temp_dir;
    let data_dir: &std::path::Path = if let Some(ref dir) = args.data_dir {
        std::path::Path::new(dir)
    } else {
        temp_dir = tempfile::TempDir::new().unwrap();
        temp_dir.path()
    };

    // Initialize storage
    let smgr = Arc::new(StorageManager::new(data_dir));
    smgr.init().expect("Failed to init storage manager");

    let mut buffer_pool = BufferPool::new(BufferPoolConfig::new(args.buffer_size))
        .expect("Failed to create buffer pool");
    buffer_pool.set_smgr(Arc::clone(&smgr));

    // Create WAL writer if --wal is enabled
    let wal_writer = if args.wal {
        let wal_dir = data_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("Failed to create WAL directory");
        println!("WAL enabled: {}", wal_dir.display());
        Some(Arc::new(ferrisdb_storage::WalWriter::new(&wal_dir)))
    } else {
        None
    };
    // Start background WAL flusher (5ms interval for tight group commit)
    let _wal_flusher = wal_writer.as_ref().map(|w| w.start_flusher(5));

    // WAL Ring Buffer（16MB 环形，后台 drain 到 WalWriter）
    let wal_ring_ref: Option<Arc<ferrisdb_storage::wal::ring_buffer::WalRingBuffer>> = if wal_writer.is_some() {
        Some(Arc::new(ferrisdb_storage::wal::ring_buffer::WalRingBuffer::new(16 * 1024 * 1024)))
    } else {
        None
    };

    // Note: WalBuffer is 128MB, enough for medium runs without flusher

    // Wire WAL writer to buffer pool (WAL-before-data enforcement)
    if let Some(ref w) = wal_writer {
        buffer_pool.set_wal_writer(Arc::clone(w));
    }
    let buffer_pool = Arc::new(buffer_pool);

    let mut txn_manager_inner = TransactionManager::new(64);
    if let Some(ref w) = wal_writer {
        txn_manager_inner.set_wal_writer(Arc::clone(w));
        txn_manager_inner.set_buffer_pool(Arc::clone(&buffer_pool));
        if let Some(ref ring) = wal_ring_ref {
            txn_manager_inner.set_wal_ring(Arc::clone(ring));
        }
    }
    let txn_manager = Arc::new(txn_manager_inner);

    // Create all TPCC tables + indexes
    let tables = Arc::new(TpccTables::new(Arc::clone(&buffer_pool), Arc::clone(&txn_manager), args.customers, wal_writer.clone(), wal_ring_ref.clone()));

    // 数据加载阶段禁用 WAL（避免 WalWriter Mutex 拖慢批量插入）
    if wal_writer.is_some() {
        tables.disable_wal();
    }

    // Load data
    let load_start = Instant::now();
    println!("Loading TPC-C data...");
    load_tpcc_data(
        &tables,
        args.warehouses,
        args.items,
        args.customers,
        args.orders,
    );
    let load_elapsed = load_start.elapsed();
    println!("Data loaded in {:.2}s", load_elapsed.as_secs_f64());
    println!();

    // 数据加载完成后启用 WAL + 启动 drain 线程
    let _wal_drain = if let (Some(ref ring), Some(ref w)) = (&wal_ring_ref, &wal_writer) {
        tables.enable_wal();
        Some(ferrisdb_storage::wal::ring_buffer::start_drain_thread(
            Arc::clone(ring), Arc::clone(w), 5,
        ))
    } else {
        if wal_writer.is_some() { tables.enable_wal(); }
        None
    };

    // Run benchmark
    let stats = Arc::new(Stats::new());
    buffer_pool.reset_stats();
    println!("Starting benchmark ({} threads, {}s)...", args.threads, args.duration);
    let bench_start = Instant::now();

    let mut handles = Vec::with_capacity(args.threads);
    for thread_id in 0..args.threads {
        let tables = Arc::clone(&tables);
        let txn_mgr = Arc::clone(&txn_manager);
        let stats = Arc::clone(&stats);
        let duration = Duration::from_secs(args.duration);
        let num_warehouses = args.warehouses;
        let item_num = args.items;

        handles.push(std::thread::spawn(move || {
            worker(
                thread_id,
                tables,
                txn_mgr,
                stats,
                num_warehouses,
                item_num,
                duration,
            );
        }));
    }

    // Progress reporter: 每 10 秒打印 TPS 快照
    let stats_reporter = Arc::clone(&stats);
    let report_secs = args.duration;
    let report_handle = std::thread::spawn(move || {
        let mut prev = 0u64;
        let mut elapsed = 0u64;
        while elapsed < report_secs {
            std::thread::sleep(Duration::from_secs(10));
            elapsed += 10;
            let current = stats_reporter.total_committed();
            let delta = current - prev;
            let tps = delta / 10;
            eprintln!("[{}s] interval TPS: {}, cumulative: {}", elapsed, tps, current / elapsed);
            prev = current;
        }
    });

    for handle in handles {
        handle.join().unwrap();
    }
    let _ = report_handle.join();

    let bench_elapsed = bench_start.elapsed();
    let run_time = bench_elapsed.as_secs();

    // Print results (matching C++ output format)
    println!();
    let total_committed = stats.total_committed();
    let total_aborted = stats.total_aborted();
    let total = total_committed + total_aborted;

    let tps = if run_time > 0 { total_committed / run_time } else { 0 };
    let new_order_committed = stats.per_txn[TXN_NEW_ORDER].committed.load(AtomicOrdering::Relaxed);
    let tpmc = if run_time > 0 { new_order_committed / run_time * 60 } else { 0 };

    println!("==> Committed TPS: {}, TPMc: {}", tps, tpmc);
    println!();

    // Header matching C++ format
    println!(
        "{:<20} | {:<20} | {:<20} | {:<20} | {:<20}",
        "tran", "#totaltran", "%ratio", "#committed", "#aborted"
    );
    println!(
        "{:<20}-+-{:-<20}-+-{:-<20}-+-{:-<20}-+-{:-<20}",
        "----", "--------", "------", "----------", "--------"
    );

    for i in 0..TXN_COUNT {
        let committed = stats.per_txn[i].committed.load(AtomicOrdering::Relaxed);
        let aborted = stats.per_txn[i].aborted.load(AtomicOrdering::Relaxed);
        let total_per = committed + aborted;
        let ratio = if total > 0 { (total_per as f64 * 100.0) / total as f64 } else { 0.0 };
        println!(
            "{:<20} | {:<20} | {:<19.1}% | {:<20} | {:<20}",
            TXN_NAMES[i], total_per, ratio, committed, aborted
        );
    }

    println!(
        "{:<20}-+-{:-<20}-+-{:-<20}-+-{:-<20}-+-{:-<20}",
        "----", "--------", "------", "----------", "--------"
    );
    let abort_pct = if total > 0 { (total_aborted as f64 * 100.0) / total as f64 } else { 0.0 };
    println!(
        "{:<20} | {:<20} | {:<19.1}% | {:<20} | {:<20}",
        "Total", total, 100.0, total_committed, total_aborted
    );
    println!("Abort rate: {:.1}%", abort_pct);

    // Buffer pool stats
    println!();
    println!("Buffer Pool:");
    println!("  Hit rate: {:.2}%", buffer_pool.hit_rate() * 100.0);
    println!("  Pins: {}", buffer_pool.stat_pins());
    println!("  Hits: {}", buffer_pool.stat_hits());
    println!("  Misses: {}", buffer_pool.stat_misses());

    // WAL stats
    if let Some(ref w) = wal_writer {
        let _ = w.sync();
        println!();
        println!("WAL:");
        println!("  File: {:08X}.wal", w.file_no());
        println!("  Offset: {} bytes", w.offset());
    }

    // Flush
    if let Err(e) = buffer_pool.flush_all() {
        eprintln!("Warning: Failed to flush buffer pool: {:?}", e);
    }
}
