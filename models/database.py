from datetime import datetime, timezone
from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = "sqlite:///./streaming.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class EventRecord(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True)
    event_type = Column(String, nullable=False, index=True)
    source = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    data = Column(JSON, default=dict)
    extra_metadata = Column(JSON, default=dict)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AggregateRecord(Base):
    __tablename__ = "aggregates"

    id = Column(String, primary_key=True)
    window_key = Column(String, nullable=False, index=True)
    window_start = Column(DateTime, nullable=False)
    window_end = Column(DateTime, nullable=False)
    event_type = Column(String, nullable=False)
    count = Column(Integer, default=0)
    sum = Column(Float, default=0.0)
    avg = Column(Float, default=0.0)
    min = Column(Float, default=0.0)
    max = Column(Float, default=0.0)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class FraudAlertRecord(Base):
    __tablename__ = "fraud_alerts"

    id = Column(String, primary_key=True)
    event_id = Column(String, nullable=False)
    rule = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
