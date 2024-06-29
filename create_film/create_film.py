import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, Enum, ForeignKey, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import DECIMAL
import os
import uuid

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

# Función Lambda para crear una nueva película
def lambda_handler(event, context):
    try:
        logger.info("Creating film")
        data = json.loads(event['body'])

        film_id = uuid.uuid4().bytes

        # Verificar la existencia de las claves necesarias en el JSON
        required_keys = ['title', 'description', 'length', 'status', 'fk_category', 'front_page', 'file']
        for key in required_keys:
            if key not in data:
                raise KeyError(f'Missing required key: {key}')

        conn = db_connection.connect()
        query = select([categories]).where(categories.c.category_id == bytes.fromhex(data['fk_category']))
        result = conn.execute(query)
        existing_category = result.fetchone()
        if not existing_category:
            conn.close()
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('Category ID does not exist')
            }

        query = films.insert().values(
            film_id=film_id,
            title=data['title'],
            description=data['description'],
            length=data['length'],
            status=data['status'],
            fk_category=bytes.fromhex(data['fk_category']),
            front_page=data['front_page'],
            file=data['file']
        )
        result = conn.execute(query)
        conn.close()
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Film created')
        }
    except SQLAlchemyError as e:
        logger.error(f"Error creating film: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error creating film')
        }
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Invalid JSON format')
        }
    except KeyError as e:
        logger.error(f"Missing required key in JSON: {e}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(f'Missing required key: {e}')
        }
