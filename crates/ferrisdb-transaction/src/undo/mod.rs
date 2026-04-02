//! Undo 模块
//!
//! Undo Zone 和 Undo Record 管理。

mod zone;
mod record;

pub use zone::UndoZone;
pub use record::{UndoRecord, UndoRecordType};
