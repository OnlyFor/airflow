from __future__ import annotations

from airflow.executors.base_executor import BaseExecutor


class NullExecutor(BaseExecutor):
    """Do nothing."""

    def __init__(self, parallelism: int = 0):
        super().__init__(parallelism=0)

    def execute_async(
        self,
        key,
        command,
        queue=None,
        executor_config=None,
    ):
        pass

    def end(self): ...

    def terminate(self): ...

    def cleanup_stuck_queued_tasks(self, tis):
        return []
