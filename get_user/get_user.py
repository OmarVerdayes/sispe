import os
import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY
from sqlalchemy.exc import SQLAlchemyError

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = os.environ.get('DBUser')
DB_PASSWORD = os.environ.get('DBPassword')
DB_NAME = os.environ.get('DBName')
DB_HOST = os.environ.get('DBHost')
#db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

db_connection_str=f'mysql+pymysql://admin:nhL5zPpY1I9w@integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com/sispe'

db_connection = create_engine(db_connection_str)
metadata = MetaData()

# Definición de la tabla de usuarios
users = Table('users', metadata,
              Column('user_id', BINARY(16), primary_key=True),
              Column('name', String(60), nullable=False),
              Column('lastname', String(60), nullable=False),
              Column('email', String(100), nullable=False),
              Column('password', String(255), nullable=False),
              Column('fk_rol', BINARY(16), nullable=False),
              Column('fk_subscription', BINARY(16), nullable=False))

# Función Lambda para obtener usuarios
def lambda_handler(event, context):
    try:
        logger.info("Fetching users")
        conn = db_connection.connect()
        query = users.select()
        result = conn.execute(query)
        user_list = [{column: value.hex() if isinstance(value, bytes) else value for column, value in row.items()} for row in result]
        conn.close()
        
        if not user_list:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('No users found')
            }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(user_list)
        }
    except SQLAlchemyError as e:
        logger.error(f"Error fetching users: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error fetching users')
        }
