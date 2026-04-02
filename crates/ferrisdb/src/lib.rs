//! FerrisDB — High-performance transactional storage engine
//!
//! This is the top-level facade crate. External callers use this as their
//! single entry point to the storage engine.
//!
//! # Quick Start
//!
//! ```ignore
//! use ferrisdb::{Engine, EngineConfig};
//!
//! let engine = Engine::open(EngineConfig::with_data_dir("/tmp/mydb"))?;
//! let table_id = engine.create_table("users")?;
//! let table = engine.open_table("users")?;
//!
//! let mut txn = engine.begin()?;
//! table.insert(b"row data", txn.xid(), 0)?;
//! txn.commit()?;
//!
//! engine.shutdown()?;
//! ```

#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod engine;
pub mod managed_table;

// Re-export the primary API
pub use engine::{Engine, EngineConfig};
pub use managed_table::{ManagedTable, IndexDef};

// Re-export commonly needed types from lower crates
pub use ferrisdb_core::{FerrisDBError, Result, Xid, Csn, Lsn};
pub use ferrisdb_core::config;
pub use ferrisdb_core::stats;
pub use ferrisdb_storage::{BTree, BTreeKey, BTreeValue};
pub use ferrisdb_transaction::{
    Transaction, TransactionManager, TransactionState,
    HeapTable, HeapScan, TupleId, TupleHeader,
};
