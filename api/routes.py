from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List

from models.database import get_db, EventRecord, AggregateRecord, FraudAlertRecord
from api.schemas import EventResponse, AggregateResponse, FraudAlertResponse

router = APIRouter()


@router.get("/events", response_model=List[EventResponse])
def list_events(
    event_type: str | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(EventRecord)
    if event_type:
        query = query.filter(EventRecord.event_type == event_type)
    if source:
        query = query.filter(EventRecord.source == source)
    events = query.order_by(desc(EventRecord.timestamp)).offset(offset).limit(limit).all()
    return events


@router.get("/events/{event_id}", response_model=EventResponse)
def get_event(event_id: str, db: Session = Depends(get_db)):
    event = db.query(EventRecord).filter(EventRecord.id == event_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@router.get("/aggregates", response_model=List[AggregateResponse])
def list_aggregates(
    event_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    query = db.query(AggregateRecord)
    if event_type:
        query = query.filter(AggregateRecord.event_type == event_type)
    aggregates = query.order_by(desc(AggregateRecord.last_updated)).limit(limit).all()
    return aggregates


@router.get("/fraud-alerts", response_model=List[FraudAlertResponse])
def list_fraud_alerts(
    severity: str | None = Query(None),
    rule: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    query = db.query(FraudAlertRecord)
    if severity:
        query = query.filter(FraudAlertRecord.severity == severity)
    if rule:
        query = query.filter(FraudAlertRecord.rule == rule)
    alerts = query.order_by(desc(FraudAlertRecord.created_at)).limit(limit).all()
    return alerts
