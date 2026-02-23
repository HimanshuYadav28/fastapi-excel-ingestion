from datetime import datetime
from sqlalchemy import Column, DateTime, Integer, String, Text
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255))
    email = Column(String(255))
    age = Column(Integer)


class UploadJob(Base):
    __tablename__ = "upload_jobs"

    job_id = Column(String(64), primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    saved_path = Column(String(512), nullable=False)
    status = Column(String(32), nullable=False, default="queued")
    chunk_size = Column(Integer, nullable=False)
    inserted_rows = Column(Integer, nullable=False, default=0)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    file_deleted_at = Column(DateTime, nullable=True)
