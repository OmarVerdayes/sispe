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
        logger.info("Getting a subscription")
        subscription_id = event['pathParameters']['subscription_id']

        conn = db_connection.connect()
        query = subscriptions.select().where(subscriptions.c.subscription_id == uuid.UUID(subscription_id).bytes)
        result = conn.execute(query).fetchone()

        if result:
            response = {
                'subscription_id': str(uuid.UUID(bytes=result['subscription_id'])),
                'start_date': result['start_date'].isoformat(),
                'end_date': result['end_date'].isoformat()
            }
            status_code = 200
        else:
            response = {'message': 'Subscription not found'}
            status_code = 404

        conn.close()
    except SQLAlchemyError as e:
        logger.error(f'Database error occurred: {e}')
        response = {'message': 'Internal Server Error'}
        status_code = 500
    
    except Exception as e:
        logger.error(f'Unexpected error occurred: {e}')
        response = {'message': 'Internal Server Error'}
        status_code = 500

    return {
        'statusCode': status_code,
        'body': json.dumps(response)
    }