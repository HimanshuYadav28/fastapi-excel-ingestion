from sqlalchemy.orm import Session
from sqlalchemy import and_
from .models import UploadJob, User
from typing import Sequence, Mapping, Any
from datetime import datetime


def bulk_insert_users(db: Session, users: Sequence[Mapping[str, Any]]):
    db.bulk_insert_mappings(User, users)
    db.commit()


def create_upload_job(
    db: Session,
    job_id: str,
    filename: str,
    saved_path: str,
    chunk_size: int,
) -> UploadJob:
    job = UploadJob(
        job_id=job_id,
        filename=filename,
        saved_path=saved_path,
        status="queued",
        chunk_size=chunk_size,
        inserted_rows=0,
        created_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_upload_job(db: Session, job_id: str) -> UploadJob | None:
    return db.query(UploadJob).filter(UploadJob.job_id == job_id).first()


def set_upload_job_running(db: Session, job_id: str) -> None:
    db.query(UploadJob).filter(UploadJob.job_id == job_id).update(
        {"status": "running", "started_at": datetime.utcnow(), "error": None}
    )
    db.commit()


def set_upload_job_completed(db: Session, job_id: str, inserted_rows: int) -> None:
    db.query(UploadJob).filter(UploadJob.job_id == job_id).update(
        {
            "status": "completed",
            "inserted_rows": inserted_rows,
            "completed_at": datetime.utcnow(),
            "error": None,
        }
    )
    db.commit()


def set_upload_job_failed(db: Session, job_id: str, error: str) -> None:
    db.query(UploadJob).filter(UploadJob.job_id == job_id).update(
        {
            "status": "failed",
            "error": error,
            "completed_at": datetime.utcnow(),
        }
    )
    db.commit()


def mark_upload_file_deleted(db: Session, job_id: str) -> None:
    db.query(UploadJob).filter(UploadJob.job_id == job_id).update(
        {"file_deleted_at": datetime.utcnow()}
    )
    db.commit()


def get_jobs_for_cleanup(db: Session, completed_before: datetime) -> list[UploadJob]:
    return (
        db.query(UploadJob)
        .filter(
            and_(
                UploadJob.completed_at.isnot(None),
                UploadJob.completed_at <= completed_before,
                UploadJob.file_deleted_at.is_(None),
                UploadJob.saved_path.isnot(None),
            )
        )
        .all()
    )


def get_upload_jobs_by_status(db: Session, statuses: Sequence[str]) -> list[UploadJob]:
    if not statuses:
        return []

    return db.query(UploadJob).filter(UploadJob.status.in_(list(statuses))).all()
