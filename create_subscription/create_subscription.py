import json
import logging
import uuid
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, DateTime
from sqlalchemy.exc import SQLAlchemyError
import os
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
DB_HOST = os.environ.get('DB_HOST')

db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)

metadata = MetaData()
subscriptions = Table('subscriptions', metadata,
                        Column('subscription_id', BINARY(16), primary_key=True),
                        Column('start_date', DateTime, nullable=False),
                        Column('end_date', DateTime, nullable=False))

def lambda_handler(event, context):
    try:
        logger.info("Creating subscription")
        data = json.loads(event['body'])

        if 'start_date' not in data or 'end_date' not in data:
            raise ValueError('The start date and end date fields are required')

        start_date = datetime.fromisoformat(data['start_date'])
        end_date = datetime.fromisoformat(data['end_date'])

        current_date = datetime.now()

        if start_date < current_date:
            raise ValueError('The start date must not be in the past')

        if start_date >= end_date:
            raise ValueError('The start date must be before the end date')

        subscription_id = uuid.uuid4().bytes
        conn = db_connection.connect()
        query = subscriptions.insert().values(subscription_id=subscription_id, start_date=start_date, end_date=end_date)
        conn.execute(query)
        conn.close()

        response = {
            'subscription_id' : subscription_id.hex(),
            'start_date': start_date.isoformat(),
            'end_date' : end_date.isoformat()
        }

        return {
            'statusCode': 201,
            'body' : json.dumps(response)
        }

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return {
            'statusCode' : 400,
            'body' : json.dumps(f"Error: {str(ve)}")
        }

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return {
            'statusCode' : 500,
            'body' : json.dumps('Internal Server Error. Could not create the subscription')
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode' : 500,
            'body' : json.dumps("Internal Server Error")
        }
