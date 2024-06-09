from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, select, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


engine = create_engine('postgresql://username:password@host:port/database_name')

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
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
)

metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def query_users(min_age, gender, min_avg_heart_rate, date_from, date_to):
    try:

        session = Session()

        avg_heart_rate_subquery = (
            select(
                heart_rates.c.user_id,
                func.avg(heart_rates.c.heart_rate).label('avg_heart_rate')
            )
            .where(
                heart_rates.c.timestamp >= date_from,
                heart_rates.c.timestamp <= date_to
            )
            .group_by(heart_rates.c.user_id)
            .alias('avg_heart_rate_subquery')
        )

        stmt = (
            select(users)
            .select_from(users.join(avg_heart_rate_subquery, users.c.id == avg_heart_rate_subquery.c.user_id))
            .where(
                func.cast(users.c.age, Integer) > min_age,
                users.c.gender == gender,
                avg_heart_rate_subquery.c.avg_heart_rate > min_avg_heart_rate
            )
        )

        result = session.execute(stmt).fetchall()

        session.close()

        return result

    except SQLAlchemyError as e:
        logger.error(f"An SQLAlchemy error has occurred: {e}")
        return {"error": f"An error occurred while executing the request: {e}"}
    
    
def query_for_user(user_id, date_from, date_to):
    try:

        session = Session()

        subquery = (
            select(
                func.avg(heart_rates.c.heart_rate).label('avg_heart_rate'),
                func.date_trunc('hour', heart_rates.c.timestamp).label('hour')
            )
            .where(
                heart_rates.c.user_id == user_id,
                heart_rates.c.timestamp >= date_from,
                heart_rates.c.timestamp <= date_to
            )
            .group_by(func.date_trunc('hour', heart_rates.c.timestamp))
            .order_by(func.avg(heart_rates.c.heart_rate).desc())
            .limit(10)
            .alias('subquery')
        )

        stmt = (
            select(subquery.c.avg_heart_rate)
            .order_by(subquery.c.hour)
        )

        result = session.execute(stmt).fetchall()

        session.close()

        return result

    except SQLAlchemyError as e:
        logger.error(f"An SQLAlchemy error has occurred: {e}")
        return {"error": f"An error occurred while executing the request: {e}"}
