import logging
import json
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
import os

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
rateings = Table('rateings', metadata, autoload_with=db_connection)

# Función Lambda para eliminar un rateing
def lambda_handler(event, context):
    try:
        logger.info("Deleting rateing")
        rateing_id = event['pathParameters']['id']

        conn = db_connection.connect()
        query = rateings.delete().where(rateings.c.rateing_id == bytes.fromhex(rateing_id))
        result = conn.execute(query)
        conn.close()

        if result.rowcount:
            return {
                'statusCode': 200,
                'body': json.dumps('Rateing deleted')
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps('Rateing not found')
            }
    except SQLAlchemyError as e:
        logger.error(f"Error deleting rateing: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error deleting rateing')
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }
