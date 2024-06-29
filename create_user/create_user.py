import os
import random
import string
import boto3
import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, UniqueConstraint, ForeignKey, Index, ForeignKeyConstraint
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import uuid

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
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
    Column('fk_subscription', BINARY(16), nullable=False),
    UniqueConstraint('email', name='unique_email'),
    ForeignKeyConstraint(['fk_rol'], ['roles.rol_id'], name='fk_rol'),
    ForeignKeyConstraint(['fk_subscription'], ['subscriptions.subscription_id'], name='fk_subscription'),
    Index('fk_rol_idx', 'fk_rol'),
    Index('fk_subscription_idx', 'fk_subscription')
)

def generate_password(length=8):
    if length < 4:
        raise ValueError("Length of the password should be at least 4")
    # Definimos los caracteres que queremos usar
    all_characters = string.ascii_letters + string.digits + string.punctuation
    password = [
        random.choice(string.ascii_lowercase),  # Al menos una letra minúscula
        random.choice(string.ascii_uppercase),  # Al menos una letra mayúscula
        random.choice(string.digits),           # Al menos un número
        random.choice(string.punctuation)       # Al menos un carácter especial
    ]
    # Rellenamos el resto de la contraseña con caracteres aleatorios
    password += random.choices(all_characters, k=length-4)
    # Mezclamos los caracteres para evitar patrones predecibles
    random.shuffle(password)
    return ''.join(password)

# Función Lambda para crear un nuevo usuario
def lambda_handler(event, context):
    logger.info("Iniciando lambda_handler")
    try:
        data = json.loads(event['body'])
        user_id = uuid.uuid4().bytes
        name = data.get('name')
        lastname = data.get('lastname')
        email = data.get('email')
        password = generate_password()
        #password = data.get('password')
        fk_rol = bytes.fromhex(data.get('fk_rol'))
        fk_subscription = bytes.fromhex(data.get('fk_subscription'))

        if not all([user_id, name, lastname, email, password, fk_rol, fk_subscription]):
            logger.error("Faltan datos obligatorios en el cuerpo de la solicitud")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('Faltan datos obligatorios')
            }

        with db_connection.connect() as connection:
            # Verificar si el email ya existe
            existing_user = connection.execute(users.select().where(users.c.email == email)).fetchone()
            if existing_user:
                logger.error(f"El correo {email} ya está registrado")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json'
                    },
                    'body': json.dumps('This email is already registered')
                }

            # Configuracion del cliente de cognito
            client = boto3.client('cognito-idp', region_name='us-east-1')
            user_pool_id = 'us-east-1_hpKh8IecL'
            logger.info(f"[CLIENTE]: {client}, [user_pool_id] {user_pool_id}]")
            # Crea el usuario con correo no verificado
            client.admin_create_user(
                UserPoolId=user_pool_id,
                Username=email,
                UserAttributes=[
                    {'Name': 'email', 'Value': email},
                    {'Name': 'email_verified', 'Value': 'false'},
                ],
                TemporaryPassword=password,
            )
            """
            INSERCION DE NUEVO USUARIO A LA BASE DE DATOS
            # Inserción de nuevo usuario a la base de datos
            insert_query = users.insert().values(
                user_id=user_id,
                name=name,
                lastname=lastname,
                email=email,
                password=password,
                fk_rol=fk_rol,
                fk_subscription=fk_subscription
            )
            connection.execute(insert_query)
            logger.info(f"Usuario {email} creado exitosamente")
            """

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps('Usuario creado exitosamente, Verifica tu correo para validar tu registro')
            }
    except IntegrityError as e:
        logger.error(f"Error de integridad: {e}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('This email is already registered')
        }
    except SQLAlchemyError as e:
        logger.error(f"Error al crear el usuario SQLAlchemy: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error al crear el usuario')
        }
    except json.JSONDecodeError as e:
        logger.error(f"Formato de JSON inválido: {e}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Formato de JSON inválido')
        }
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Error interno del servidor')
        }
    finally:
        logger.info("Ejecución de lambda_handler completada")
