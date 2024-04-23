use crate::lsm_storage::LsmStorageState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let sizes = snapshot
            .levels
            .iter()
            .map(|x| x.1.len())
            .collect::<Vec<_>>();
        let last_index = sizes.len() - 1;
        let space_ratio =
            sizes[..last_index].iter().sum::<usize>() as f64 / sizes[last_index] as f64 * 100.0;
        if space_ratio >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        let size_ratio = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut sum = 0;
        for i in 0..last_index {
            sum += sizes[i];
            if sum as f64 / sizes[i + 1] as f64 >= size_ratio
                && i + 2 >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..i + 2].to_vec(),
                    bottom_tier_included: i + 2 == snapshot.levels.len(),
                });
            }
        }

        let top_most = snapshot.levels.len() - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..top_most].to_vec(),
            bottom_tier_included: snapshot.levels.len() == top_most,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut i = 0;
        while i < snapshot.levels.len() && snapshot.levels[i].0 != task.tiers[0].0 {
            i += 1;
        }
        drop(snapshot.levels.drain(i..i + task.tiers.len()));
        snapshot.levels.insert(i, (output[0], output.to_vec()));
        let remove = task
            .tiers
            .iter()
            .flat_map(|x| x.1.clone().into_iter())
            .collect();
        (snapshot, remove)
    }
}
