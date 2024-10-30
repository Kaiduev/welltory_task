import logging
from datetime import datetime
from typing import List, Tuple

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, select, \
    func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://username:password@host:port/database_name'
engine = create_engine(DATABASE_URL)
metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('gender', String),
    Column('age', String)
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime, index=True),
    Column('heart_rate', Float),
)

metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def log_sqlalchemy_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SQLAlchemyError as e:
            logger.error(f"An SQLAlchemy error has occurred: {e}")
            return {"error": f"An error occurred while executing the request: {e}"}
    return wrapper


@log_sqlalchemy_error
def query_users(min_age: int, gender: str, min_avg_heart_rate: float, date_from: datetime, date_to: datetime) -> List[Tuple]:
    with Session() as session:
        avg_heart_rate = (
            select(
                heart_rates.c.user_id,
                func.avg(heart_rates.c.heart_rate).label('avg_heart_rate')
            )
            .where(
                heart_rates.c.timestamp.between(date_from, date_to)
            )
            .group_by(heart_rates.c.user_id)
        ).alias('avg_heart_rate_subquery')

        stmt = (
            select(users)
            .join(avg_heart_rate, users.c.id == avg_heart_rate.c.user_id)
            .where(
                func.cast(users.c.age, Integer) > min_age,
                users.c.gender == gender,
                avg_heart_rate.c.avg_heart_rate > min_avg_heart_rate
            )
        )

        return session.execute(stmt).fetchall()


@log_sqlalchemy_error
def query_for_user(user_id: int, date_from: datetime, date_to: datetime) -> List[Tuple]:
    with Session() as session:
        subquery = (
            select(
                func.avg(heart_rates.c.heart_rate).label('avg_heart_rate'),
                func.date_trunc('hour', heart_rates.c.timestamp).label('hour')
            )
            .where(
                heart_rates.c.user_id == user_id,
                heart_rates.c.timestamp.between(date_from, date_to)
            )
            .group_by(func.date_trunc('hour', heart_rates.c.timestamp))
            .order_by(func.avg(heart_rates.c.heart_rate).desc())
            .limit(10)
        )

        stmt = select(subquery.c.avg_heart_rate).order_by(subquery.c.hour)
        return session.execute(stmt).fetchall()
