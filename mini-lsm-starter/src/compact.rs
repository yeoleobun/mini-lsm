#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::NoCompaction, _) => (snapshot.clone(), Vec::new()),
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn do_compact(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        skip_empty: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut res = Vec::new();
        while iter.is_valid() {
            if skip_empty && iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            builder.add(iter.key(), iter.value());
            if builder.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let path = self.path_of_sst(id);
                if !path.exists() {
                    File::create(path.clone())?;
                }
                let table = builder.build(id, None, path)?;
                res.push(Arc::new(table));
                builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }
        if builder.estimated_size() > 0 {
            let id = self.next_sst_id();
            let path = self.path_of_sst(id);
            File::create(path.clone())?;
            let table = builder.build(id, None, path)?;
            res.push(Arc::new(table));
        }
        Ok(res)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(task) => {
                let snapshot = self.state.read().clone();
                let lower_ssts = task
                    .lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                if task.upper_level.is_none() {
                    let mut sst_iters = Vec::new();
                    for id in &task.upper_level_sst_ids {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        let iter = SsTableIterator::create_and_seek_to_first(table)?;
                        sst_iters.push(Box::new(iter));
                    }
                    let upper_iter = MergeIterator::create(sst_iters);
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.do_compact(iter, task.is_lower_level_bottom_level)
                } else {
                    let upper_ssts = task
                        .upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.do_compact(iter, task.is_lower_level_bottom_level)
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter = {
                    let state = self.state.read();
                    let mut l0_iters = Vec::new();
                    for id in l0_sstables {
                        let table = state.sstables.get(id).unwrap().clone();
                        let iter = SsTableIterator::create_and_seek_to_first(table)?;
                        l0_iters.push(Box::new(iter));
                    }
                    let l0_merge = MergeIterator::create(l0_iters);

                    let l1_ssts = l1_sstables
                        .iter()
                        .map(|id| state.sstables.get(id).unwrap().clone())
                        .collect();
                    let l1_concat = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;
                    TwoMergeIterator::create(l0_merge, l1_concat)?
                };

                self.do_compact(iter, true)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let l0;
        let l1;
        {
            let state = self.state.read();
            l0 = state.l0_sstables.clone();
            l1 = state.levels[0].1.clone();
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0.clone(),
            l1_sstables: l1.clone(),
        };
        let new_ssts = self.compact(&task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let state = Arc::get_mut(&mut guard).unwrap();
            let n = state.l0_sstables.len() - l0.len();
            state.l0_sstables.truncate(n);
            for table in &new_ssts {
                state.sstables.insert(table.sst_id(), Arc::clone(table));
            }
            let sst_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            state.levels[0] = (1, sst_ids);
            for id in l0.iter().chain(l1.iter()) {
                state.sstables.remove(id);
            }
        };
        for id in l0.iter().chain(l1.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // let snapshot = ;
        let task = self
            .compaction_controller
            .generate_compaction_task(&self.state.read());
        let Some(task) = task else { return Ok(()) };
        let ssts = self.compact(&task)?;
        let output: Vec<usize> = ssts.iter().map(|sst| sst.sst_id()).collect();
        let remove = {
            let mut remove = Vec::new();
            let _guard = self.state_lock.lock();
            let read_gurad = self.state.read();
            let snapshot = read_gurad.as_ref().clone();
            let (mut snapshot, delete) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output);
            for id in delete {
                snapshot.sstables.remove(&id);
                remove.push(self.path_of_sst(id));
            }
            for sst in ssts {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            drop(read_gurad);
            let mut write_guard = self.state.write();
            *write_guard = Arc::new(snapshot);
            remove
        };
        for path in remove {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
