use ps_promise::{PromiseRejection, TaskFailure};

use crate::error::DataLakeError;

impl PromiseRejection for DataLakeError {
    fn already_consumed() -> Self {
        Self::PromiseConsumedAlready
    }

    fn task_failed(failure: TaskFailure) -> Self {
        Self::TaskFailed(failure)
    }
}
