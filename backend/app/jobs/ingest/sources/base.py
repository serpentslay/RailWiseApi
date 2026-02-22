from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
import uuid

class BaseSource(ABC):
    @abstractmethod
    def ingest(self, db: Session, run_id: uuid.UUID, **kwargs) -> dict:
        """
        Implement fetch -> normalize -> load. Return dict of metrics for job_runs.meta
        """
        raise NotImplementedError
