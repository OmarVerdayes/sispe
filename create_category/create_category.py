import json
import logging
import uuid
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY
from sqlalchemy.exc import SQLAlchemyError
import os

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
DB_HOST = os.environ.get('DB_HOST')
# Cadena de conexión
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)

metadata = MetaData()
categories = Table('categories', metadata,
                   Column('category_id', BINARY(16), primary_key=True),
                   Column('name', String(45), nullable=False))

def lambda_handler(event, context):
    try:
        logger.info("Creating category")
        data = json.loads(event['body'])
        
        if 'name' not in data:
            raise ValueError('The name field is required.')
        
        category_id = uuid.uuid4().bytes
        conn = db_connection.connect()
        query = categories.insert().values(category_id=category_id, name=data['name'])
        conn.execute(query)
        conn.close()
        
        return {
            'statusCode': 201,
            'body': json.dumps({'category_id': category_id.hex()})
        }
        
    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Error: {str(ve)}")
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error. Could not create the category.')
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error.')
        }