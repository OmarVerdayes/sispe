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
        logger.info("Updating category")

        # Verificar que el usuario sea un administrador
        claims = event['requestContext']['authorizer']['claims']
        groups = claims.get('cognito:groups', [])

        if 'admin' not in groups:
            logger.warning("User does not have permission to update categories")
            return {
                'statusCode': 403,
                'body': json.dumps('Forbidden: You do not have permission to update categories.')
            }

        # Extraer category_id de los parámetros de la ruta
        category_id = event['pathParameters']['category_id']

        data = json.loads(event['body'])

        if 'name' not in data:
            raise ValueError('The name field is required.')

        conn = db_connection.connect()
        query = categories.update().where(categories.c.category_id == bytes.fromhex(category_id)).values(
            name=data['name'])
        conn.execute(query)
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps('Categoría actualizada')
        }

    except KeyError:
        logger.error("category_id is missing in the path parameters")
        return {
            'statusCode': 400,
            'body': json.dumps('Error: category_id is required in the path parameters.')
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
            'body': json.dumps('Internal server error. Could not update the category.')
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error.')
        }