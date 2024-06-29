import json
import logging
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, UniqueConstraint, ForeignKey, Index, ForeignKeyConstraint

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_ACCESS_KEY_ID='AKIAR7477EOW7CWUCEHJ'
AWS_SECRET_ACCESS_KEY='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'
db_connection_str = 'mysql+pymysql://admin:nhL5zPpY1I9w@integradora-lambda.czc42euyq8iq.us-east-1.rds.amazonaws.com/sispe'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

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

def lambda_handler(event, __):
    client = boto3.client('cognito-idp', region_name='us-east-1',aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    user_pool_id = "us-east-1_hpKh8IecL"
    client_id = "42dmr7oq6bkfoufph3q36paqta"
    try:

        # Parsea el body del evento
        body_parameters = json.loads(event["body"])
        email = body_parameters.get('email')
        temporary_password = body_parameters.get('temporary_password')
        new_password = body_parameters.get('new_password')

        # Autentica al usuario con la contraseña temporal
        response = client.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow='ADMIN_USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': email,
                'PASSWORD': temporary_password
            }
        )
        logging.info(f"[RESPONSE] {response}")

        if response['ChallengeName'] == 'NEW_PASSWORD_REQUIRED':
            client.respond_to_auth_challenge(
                ClientId=client_id,
                ChallengeName='NEW_PASSWORD_REQUIRED',
                Session=response['Session'],
                ChallengeResponses={
                    'USERNAME': email,
                    'NEW_PASSWORD': new_password,
                    'email_verified': 'true'
                }
            )
            update_query = users.update().where(users.c.email == email).values(
                password=new_password
            )
            conn= db_connection.connect()
            response = conn.execute(update_query)
            return {
                'statusCode': 200,
                'body': json.dumps({"message": "Password changed successfully."})
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({"error_message": "Unexpected challenge."})
            }

    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({"error_message": e.response['Error']['Message']})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error_message": str(e)})
        }