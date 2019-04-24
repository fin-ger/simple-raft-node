use crate::{
    Storage,
    Snapshot,
    SnapshotMetadata,
    SnapshotReadError,
    SnapshotWriteError,
    Entry,
    EntryReadError,
    EntryWriteError,
};

#[derive(Default)]
pub struct MemStorage {
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

impl Storage for MemStorage {
    type InitError = ();

    fn init<IntoString: Into<String>>(
        &mut self,
        _node_name: IntoString,
    ) -> Result<(), Self::InitError> {
        // we ignore the node_name here as we don't need an identifier for the node to load
        // old state e.g. from disk
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

        if self.first_index() > metadata.get_index() {
            return Err(SnapshotWriteError::SnapshotOutOfDate);
        }

        self.snapshot_metadata = metadata.clone();
        self.snapshot = snapshot;
        self.entries.clear();

        Ok(())
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), EntryWriteError>{
        if entries.is_empty() {
            return Ok(());
        }

        if self.first_index() > entries[0].get_index() {
            return Err(EntryWriteError::AlreadyCompacted {
                compacted_index: self.first_index() - 1,
                entry_index: entries[0].get_index(),
            });
        }

        if self.last_index() + 1 < entries[0].get_index() {
            return Err(EntryWriteError::NotContinuous {
                last_entry: self.last_index(),
                entry_index: entries[0].get_index(),
            });
        }

        let diff = entries[0].get_index() - self.first_index();
        self.entries.drain(diff as usize..);
        self.entries.extend_from_slice(&entries);

        Ok(())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, EntryReadError> {
        if let Some(offset) = self.try_first_index() {
            if low >= offset {
                if high > self.last_index() + 1 {
                    return Err(EntryReadError::OutOfBounds);
                }

                let lo = (low - offset) as usize;
                let hi = (high - offset) as usize;
                let mut entries = self.entries[lo..hi].to_vec();
                raft::util::limit_size(&mut entries, max_size.into());

                return Ok(entries);
            }
        }

        Err(EntryReadError::Compacted)
    }

    fn try_first_index(&self) -> Option<u64> {
        self.entries.first()
            .map(|entry| entry.get_index())
    }

    fn try_last_index(&self) -> Option<u64> {
        self.entries.last()
            .map(|entry| entry.get_index())
    }
}
