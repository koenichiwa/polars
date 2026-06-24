use polars_ops::prelude::*;
use recursive::recursive;

use super::*;

pub(crate) struct MergeSorted {
    pub(crate) input_left: Box<dyn Executor>,
    pub(crate) input_right: Box<dyn Executor>,
    pub(crate) key: Vec<PlSmallStr>,
}

impl Executor for MergeSorted {
    #[recursive]
    fn execute(&mut self, state: &mut ExecutionState) -> PolarsResult<DataFrame> {
        state.should_stop()?;
        #[cfg(debug_assertions)]
        {
            if state.verbose() {
                eprintln!("run MergeSorted")
            }
        }
        let (left, right) = {
            let mut state2 = state.split();
            state2.branch_idx += 1;
            let (left, right) = RAYON.join(
                || self.input_left.execute(state),
                || self.input_right.execute(&mut state2),
            );
            (left?, right?)
        };

        let profile_name = Cow::Borrowed("Merge Sorted");
        state.record(
            || {
                let key_s = self
                    .key
                    .iter()
                    .map(|key| {
                        Ok((
                            left.column(key.as_str())?.as_materialized_series(),
                            right.column(key.as_str())?.as_materialized_series(),
                        ))
                    })
                    .collect::<Result<Vec<(_, _)>, PolarsError>>()?;

                _merge_sorted_dfs(&left, &right, &key_s, true)
            },
            profile_name,
        )
    }
}
