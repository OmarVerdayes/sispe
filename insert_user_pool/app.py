import os
import random
import string
import boto3
import logging
import json
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, UniqueConstraint, ForeignKey, Index, ForeignKeyConstraint
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import uuid
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Configuración de la base de datos
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
DB_HOST = os.environ.get('DB_HOST')
#db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection_str = 'mysql+pymysql://admin:nhL5zPpY1I9w@integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com/sispe'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

AWS_ACCESS_KEY_ID='AKIAR7477EOW7CWUCEHJ'
AWS_SECRET_ACCESS_KEY='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'

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
    special_characters = ',/$@'
    all_characters = string.ascii_letters + string.digits + special_characters
    password = [
        random.choice(string.ascii_lowercase),  # Al menos una letra minúscula
        random.choice(string.ascii_uppercase),  # Al menos una letra mayúscula
        random.choice(string.digits),           # Al menos un número
        random.choice(special_characters)       # Al menos un carácter especial
    ]
    # Rellenamos el resto de la contraseña con caracteres aleatorios
    password += random.choices(all_characters, k=length-4)
    # Mezclamos los caracteres para evitar patrones predecibles
    random.shuffle(password)
    return ''.join(password)


def lambda_handler(event, context):
    data = json.loads(event['body'])
    user_id = uuid.uuid4().bytes
    name = data.get('name')
    lastname = data.get('lastname')
    email = data.get('email')
    password = generate_password()
    # password = data.get('password')
    fk_rol = bytes.fromhex(data.get('fk_rol'))
    fk_subscription = bytes.fromhex(data.get('fk_subscription'))
    role="cliente"

    if name is None or lastname is None or email is None or fk_rol is None or fk_subscription is None:
        logger.error("Faltan datos obligatorios en el cuerpo de la solicitud")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Faltan datos obligatorios')
        }

    try:
        conn = db_connection.connect()
        client = boto3.client('cognito-idp',region_name='us-east-1',aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        user_pool_id= 'us-east-1_hpKh8IecL'
        client.admin_create_user(
            UserPoolId=user_pool_id,
            Username=email,
            UserAttributes=[
                {'Name': 'email', 'Value': email},
                {'Name': 'email_verified', 'Value': 'false'},
            ],
            TemporaryPassword=password,
        )

        client.admin_add_user_to_group(
            UserPoolId=user_pool_id,
            Username=email,
            GroupName=role,
        )

        insert_query = users.insert().values(
            user_id=user_id,
            name=name,
            lastname=lastname,
            email=email,
            password=password,
            fk_rol=fk_rol,
            fk_subscription=fk_subscription
        )
        conn = db_connection.connect()
        response = conn.execute(insert_query)
        logging.info(f"[RESPONSE]: {response}")
        conn.close()
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps('Usuario registrado, verifica tu correo electronico')
        }
    except ClientError as e:

        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({"error_message": e.response['Error']['Message']})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({"error_message": str(e)})
        }