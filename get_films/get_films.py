import logging
import json
from decimal import Decimal
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, Enum, ForeignKey
from sqlalchemy.types import DECIMAL
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
# db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

db_connection_str = f'mysql+pymysql://admin:nhL5zPpY1I9w@integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com/sispe'

db_connection = create_engine(db_connection_str)
metadata = MetaData()

# Definición de la tabla de categorías
categories = Table('categories', metadata,
                   Column('category_id', BINARY(16), primary_key=True),
                   Column('name', String(45), nullable=False))

# Definición de la tabla de films
films = Table('films', metadata,
              Column('film_id', BINARY(16), primary_key=True),
              Column('title', String(60), nullable=False),
              Column('description', String(255), nullable=False),
              Column('length', DECIMAL(4, 2), nullable=False),
              Column('status', Enum('Activo', 'Inactivo', name='status_enum'), nullable=False),
              Column('fk_category', BINARY(16), ForeignKey('categories.category_id'), nullable=False),
              Column('front_page', String(255), nullable=False),
              Column('file', String(255), nullable=False)
              )

# Función Lambda para obtener películas
def lambda_handler(event, context):
    try:
        logger.info("Fetching films")
        conn = db_connection.connect()
        query = films.select()
        result = conn.execute(query)
        film_list = [
            {column: value.hex() if isinstance(value, bytes) else (float(value) if isinstance(value, Decimal) else value)
             for column, value in row.items()}
            for row in result
        ]
        conn.close()

        if not film_list:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('No films found')
            }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(film_list)
        }
    except SQLAlchemyError as e:
        logger.error(f"Error fetching films: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error fetching films')
        }
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(f'Exception: {str(e)}')
        }
