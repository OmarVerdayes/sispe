import json
import boto3
from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID='AKIAR7477EOW7CWUCEHJ'
AWS_SECRET_ACCESS_KEY='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'

def lambda_handler(event, __):
    client = boto3.client('cognito-idp', region_name='us-east-1', aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    client_id = "42dmr7oq6bkfoufph3q36paqta"

    try:
        body_parameters = json.loads(event["body"])
        email = body_parameters.get('email')
        password = body_parameters.get('password')

        response = client.initiate_auth(
            ClientId=client_id,
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': email,
                'PASSWORD': password
            }
        )

        id_token = response['AuthenticationResult']['IdToken']
        access_token = response['AuthenticationResult']['AccessToken']
        refresh_token = response['AuthenticationResult']['RefreshToken']

        # Obtén los grupos del usuario
        user_groups = client.admin_list_groups_for_user(
            Username=email,
            UserPoolId='us-east-1_hpKh8IecL'  # Reemplaza con tu User Pool ID
        )

        # Determina el rol basado en el grupo
        role = None
        if user_groups['Groups']:
            role = user_groups['Groups'][0]['GroupName']  # Asumiendo un usuario pertenece a un solo grupo

        return {
            'statusCode': 200,
            'body': json.dumps({
                'id_token': id_token,
                'access_token': access_token,
                'refresh_token': refresh_token,
                'role': role
            })
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