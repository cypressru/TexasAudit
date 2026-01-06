"""Database connection and session management."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from fraudit.config import config
from .models import Base


_engine = None
_SessionLocal = None


def get_engine():
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            config.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _engine


def get_session_factory():
    """Get or create the session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Get a database session context manager."""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Initialize the database by creating all tables."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)


def drop_db() -> None:
    """Drop all tables. Use with caution!"""
    engine = get_engine()
    Base.metadata.drop_all(bind=engine)
