pub use raft::eraftpb::{Snapshot, SnapshotMetadata, Entry};
pub use raft::eraftpb::{ConfState, HardState};
use raft::storage::RaftState;
use failure::Fail;

#[derive(Debug, Fail)]
pub enum EntryReadError {
    #[fail(display = "The entry was compacted into a snapshot and is not accessible any more!")]
    Compacted,
    #[fail(display = "Entry index is out of bounds")]
    OutOfBounds,
}

#[derive(Debug, Fail)]
pub enum SnapshotReadError {
    #[fail(display = "The snapshot is temporarily unavailable, but may be available later on.")]
    SnapshotTemporarilyUnavailable,
}

#[derive(Debug, Fail)]
#[fail(display = "The state cannot be read from the storage!")]
pub struct ReadError;

#[derive(Debug, Fail)]
pub enum EntryWriteError {
    #[fail(display = "Cannot overwrite already compacted raft logs! The log is compacted until index {}, but new entry has index {}.", compacted_index, entry_index)]
    AlreadyCompacted {
        compacted_index: u64,
        entry_index: u64,
    },
    #[fail(display = "The new entry is not continuous to the last known log entry! The last entry of the log is {}, but the new entry is {}.", last_entry, entry_index)]
    NotContinuous {
        last_entry: u64,
        entry_index: u64,
    },
    #[fail(display = "The entry could not be written by the storage!")]
    Write,
}

#[derive(Debug, Fail)]
pub enum SnapshotWriteError {
    #[fail(display = "The snapshot that was about to be applied is out of date!")]
    OutOfDate,
    #[fail(display = "The snapshot could not be written by the storage!")]
    Write,
}

#[derive(Debug, Fail)]
#[fail(display = "The state could not be written by the storage!")]
pub struct WriteError;

pub trait Storage: Default + Send + Sized {
    type InitError: Fail;

    fn init(&mut self, node_id: u64) -> Result<(), Self::InitError>;
    fn hard_state<'a>(&'a self) -> &'a HardState;
    fn set_hard_state(&mut self, hard_state: HardState) -> Result<(), WriteError>;
    fn conf_state<'a>(&'a self) -> &'a ConfState;
    fn set_conf_state(&mut self, conf_state: ConfState) -> Result<(), WriteError>;
    fn snapshot(&self) -> Result<Snapshot, SnapshotReadError>;
    fn snapshot_metadata<'a>(&'a self) -> &'a SnapshotMetadata;
    fn set_snapshot(&mut self, snapshot: Snapshot) -> Result<(), SnapshotWriteError>;
    fn append(&mut self, entries: &[Entry]) -> Result<(), EntryWriteError>;
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, EntryReadError>;
    fn try_first_index(&self) -> Option<u64>;
    fn try_last_index(&self) -> Option<u64>;

    fn init_with_conf_state<IntoConfState: Into<ConfState>>(
        &mut self,
        node_id: u64,
        conf_state: IntoConfState,
    ) -> Result<(), Self::InitError> {
        let mut metadata: SnapshotMetadata = Default::default();
        metadata.set_index(1);
        metadata.set_term(1);
        metadata.set_conf_state(conf_state.into());
        let mut snapshot: Snapshot = Default::default();
        snapshot.set_metadata(metadata);

        self.init(node_id)?;
        // we can unwrap as snapshot will never be out of date
        self.set_snapshot(snapshot).unwrap();

        Ok(())
    }

    fn first_index(&self) -> u64 {
        match self.try_first_index() {
            Some(idx) => idx,
            None => self.snapshot_metadata().get_index() + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.try_last_index() {
            Some(idx) => idx,
            None => self.snapshot_metadata().get_index(),
        }
    }
}

#[derive(Default)]
pub struct WrappedStorage<T: Storage> {
    storage: T,
}

impl<T: Storage> WrappedStorage<T> {
    pub fn new(storage: T) -> Self {
        Self { storage }
    }

    pub fn writable(&mut self) -> &mut T {
        &mut self.storage
    }
}

impl<T: Storage> raft::Storage for WrappedStorage<T> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(
            self.storage.hard_state().clone(),
            self.storage.conf_state().clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        self.storage.entries(low, high, max_size)
            .map_err(|e| {
                match e {
                    EntryReadError::Compacted => raft::StorageError::Compacted.into(),
                    e => panic!("{}", e),
                }
            })
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let metadata = self.storage.snapshot_metadata();
        if idx == metadata.get_index() {
            return Ok(metadata.get_term());
        }

        if idx < self.storage.first_index() {
            return Err(raft::StorageError::Compacted.into());
        }

        if let Some(first) = self.storage.try_first_index() {
            if let Some(last) = self.storage.try_last_index() {
                if idx >= first && idx <= last {
                    return Ok(
                        self.storage
                            .entries(idx, idx + 1, 1)
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .get_term()
                    );
                }
            }
        }

        Err(raft::StorageError::Unavailable.into())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.storage.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.storage.last_index())
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        match self.storage.snapshot() {
            Ok(snapshot) => Ok(snapshot),
            Err(SnapshotReadError::SnapshotTemporarilyUnavailable) => Err(
                raft::StorageError::SnapshotTemporarilyUnavailable.into()
            ),
        }
    }
}
