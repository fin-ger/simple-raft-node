use failure::{Fail, Backtrace};

use crate::{
    Storage,
    ConfState,
    HardState,
    WriteError,
    Snapshot,
    SnapshotMetadata,
    SnapshotReadError,
    SnapshotWriteError,
    Entry,
    EntryReadError,
    EntryWriteError,
};

#[derive(Debug, Fail)]
#[fail(display = "memory storage failed (this will never happen)")]
pub struct MemStorageError {
    backtrace: Backtrace,
}

#[derive(Default)]
pub struct MemStorage {
    hard_state: HardState,
    conf_state: ConfState,
    snapshot: Snapshot,
    // We store the metadata explicitly as it is recommended for other storage
    // implementations that use this code as a reference. Although the metadata
    // is also included in the snapshot itself, it is better to cache it in
    // memory (**not** on disk) for better performance when only the snapshot
    // metadata is needed. Otherwise, the whole snapshot would need to be read
    // from disk or cached as a whole in memory, which can get very large for
    // complex state machines.
    snapshot_metadata: SnapshotMetadata,
    entries: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Storage for MemStorage {
    type InitError = MemStorageError;

    fn init(&mut self, _node_id: u64) -> Result<(), Self::InitError> {
        // we ignore the node_name here as we don't need an identifier for the node to load
        // old state e.g. from disk
        log::debug!("initializing memory storage");
        Ok(())
    }

    fn hard_state<'a>(&'a self) -> &'a HardState {
        &self.hard_state
    }

    fn set_hard_state(&mut self, hard_state: HardState) -> Result<(), WriteError> {
        self.hard_state = hard_state;
        Ok(())
    }

    fn conf_state<'a>(&'a self) -> &'a ConfState {
        &self.conf_state
    }

    fn set_conf_state(&mut self, conf_state: ConfState) -> Result<(), WriteError> {
        self.conf_state = conf_state;
        Ok(())
    }

    fn snapshot(&self) -> Result<Snapshot, SnapshotReadError> {
        Ok(self.snapshot.clone())
    }

    fn snapshot_metadata(&self) -> &SnapshotMetadata {
        &self.snapshot_metadata
    }

    fn set_snapshot(&mut self, snapshot: Snapshot) -> Result<(), SnapshotWriteError> {
        let metadata = snapshot.get_metadata();
        log::debug!("applying snapshot on memory storage...");

        if self.first_index() > metadata.index {
            log::error!("snapshot is out-of-date and cannot be applied");
            return Err(SnapshotWriteError::OutOfDate(Backtrace::new()));
        }

        self.snapshot_metadata = metadata.clone();
        self.hard_state.set_term(metadata.term);
        self.hard_state.set_commit(metadata.index);
        self.conf_state = metadata.get_conf_state().clone();
        self.snapshot = snapshot;
        self.entries.clear();

        log::debug!("snapshot applied");

        Ok(())
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), EntryWriteError>{
        if entries.is_empty() {
            return Ok(());
        }

        log::trace!("appending {} new entries to memory storage...", entries.len());

        if self.first_index() > entries[0].index {
            log::error!("cannot apply new entries as the indices are already compacted");
            return Err(EntryWriteError::AlreadyCompacted {
                compacted_index: self.first_index() - 1,
                entry_index: entries[0].index,
                backtrace: Backtrace::new(),
            });
        }

        if self.last_index() + 1 < entries[0].index {
            log::error!("cannot apply new entries as the first index is not continuous");
            return Err(EntryWriteError::NotContinuous {
                last_entry: self.last_index(),
                entry_index: entries[0].index,
                backtrace: Backtrace::new(),
            });
        }

        let diff = entries[0].index - self.first_index();
        self.entries.drain(diff as usize..);
        self.entries.extend_from_slice(&entries);

        log::trace!("appended {} entries to the memory storage", entries.len());

        Ok(())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, EntryReadError> {
        let max_size = max_size.into();

        log::trace!(
            "reading entries {} to {} with {:?} boundary from memory storage...",
            low,
            high,
            max_size,
        );

        if let Some(offset) = self.try_first_index() {
            if low >= offset {
                if high > self.last_index() + 1 {
                    log::error!("the given entry indices ({}, {}) are out of bounds", low, high);
                    return Err(EntryReadError::OutOfBounds(Backtrace::new()));
                }

                let lo = (low - offset) as usize;
                let hi = (high - offset) as usize;
                let mut entries = self.entries[lo..hi].to_vec();
                raft::util::limit_size(&mut entries, max_size.into());

                return Ok(entries);
            }
        }

        log::error!("part of the given entry indices ({}, {}) are already compacted", low, high);

        Err(EntryReadError::Compacted(Backtrace::new()))
    }

    fn try_first_index(&self) -> Option<u64> {
        self.entries.first()
            .map(|entry| entry.index)
    }

    fn try_last_index(&self) -> Option<u64> {
        self.entries.last()
            .map(|entry| entry.index)
    }
}
