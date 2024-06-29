import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY
from sqlalchemy.exc import SQLAlchemyError
import os

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = os.environ.get("DBUser")
DB_PASSWORD = os.environ.get("DBPassword")
DB_NAME = os.environ.get("DBName")
DB_HOST = os.environ.get("DBHost")
#db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection_str = 'mysql+pymysql://admin:nhL5zPpY1I9w@integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com/sispe'

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

# Función Lambda para eliminar un usuario existente
def lambda_handler(event, context):
    try:
        logger.info("Deleting user")

        # Obtener user_id de los parámetros de la ruta
        user_id = event.get('pathParameters', {}).get('user_id')
        if not user_id:
            logger.error("El parámetro 'user_id' es obligatorio")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps("El parámetro 'user_id' es obligatorio")
            }

        conn = db_connection.connect()
        query = users.select().where(users.c.user_id == bytes.fromhex(user_id))
        result = conn.execute(query)
        existing_user = result.fetchone()

        if not existing_user:
            conn.close()
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('User not found')
            }

        query = users.delete().where(users.c.user_id == bytes.fromhex(user_id))
        conn.execute(query)
        conn.close()

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Usuario eliminado')
        }
    except SQLAlchemyError as e:
        logger.error(f"Error deleting user: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error deleting user')
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error interno del servidor')
        }
    finally:
        logger.info("Ejecución de lambda_handler completada")
