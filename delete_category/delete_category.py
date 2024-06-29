import json
import logging
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
        logger.info("Deleting category")
        
        # Extraer category_id de los parámetros de la ruta
        category_id = event['pathParameters']['category_id']
        
        conn = db_connection.connect()
        query = categories.delete().where(categories.c.category_id == bytes.fromhex(category_id))
        result = conn.execute(query)
        conn.close()
        
        if result.rowcount == 0:
            return {
                'statusCode': 404,
                'body': json.dumps('Category not found')
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps('Categoría eliminada')
        }
        
    except KeyError:
        logger.error("category_id is missing in the path parameters")
        return {
            'statusCode': 400,
            'body': json.dumps('Error: category_id is required in the path parameters.')
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error. Could not delete the category.')
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error.')
        }
