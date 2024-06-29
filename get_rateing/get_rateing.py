import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, BINARY, DECIMAL, VARCHAR
from sqlalchemy.exc import SQLAlchemyError
from decimal import Decimal

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = 'admin'
DB_PASSWORD = 'nhL5zPpY1I9w'
DB_NAME = 'sispe'
DB_HOST = 'integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com'
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

# Definición de la tabla de rateings
rateings = Table('rateings', metadata,
                 Column('rateing_id', BINARY(16), primary_key=True),
                 Column('grade', DECIMAL(2,1), nullable=False),
                 Column('comment', VARCHAR(255)),
                 Column('fk_user', BINARY(16)),
                 Column('fk_film', BINARY(16)))

# Función personalizada para manejar la conversión de tipos no serializables
def custom_json_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# Función Lambda para obtener todos los rateings
def lambda_handler(event, context):
    try:
        logger.info("Fetching rateings")
        conn = db_connection.connect()
        query = rateings.select()
        result = conn.execute(query)
        rateing_list = [
            {column: custom_json_converter(value) if isinstance(value, (Decimal, bytes)) else value for column, value in row.items()}
            for row in result
        ]
        conn.close()

        if not rateing_list:
            return {
                'statusCode': 404,
                'body': json.dumps('No rateings found')
            }

        return {
            'statusCode': 200,
            'body': json.dumps(rateing_list)
        }
    except SQLAlchemyError as e:
        logger.error(f"Error fetching rateings: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error fetching rateings')
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }
