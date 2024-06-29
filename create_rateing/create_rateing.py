import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, DECIMAL, String, BINARY
from sqlalchemy.exc import SQLAlchemyError
import os
import uuid

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

# Definición de la tabla de rateings
rateings = Table('rateings', metadata,
                 Column('rateing_id', BINARY(16), primary_key=True),
                 Column('grade', DECIMAL(2,1), nullable=False),
                 Column('comment', String(255), nullable=True),
                 Column('fk_user', BINARY(16), nullable=False),
                 Column('fk_film', BINARY(16), nullable=False))

# Función Lambda para crear un nuevo rateing
def lambda_handler(event, context):
    try:
        logger.info("Creating rateing")
        data = json.loads(event['body'])

        rateing_id = uuid.uuid4().bytes
        conn = db_connection.connect()
        query = rateings.insert().values(
            rateing_id=rateing_id,
            grade=data['grade'],
            comment=data.get('comment'),
            fk_user=bytes.fromhex(data['fk_user']),
            fk_film=bytes.fromhex(data['fk_film'])
        )
        conn.execute(query)
        conn.close()
        return {
            'statusCode': 200,
            'body': json.dumps('Rateing creado')
        }
    except SQLAlchemyError as e:
        logger.error(f"Error creating rateing: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error creating rateing')
        }
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid JSON format')
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }
