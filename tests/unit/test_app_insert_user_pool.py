import json
import unittest
from insert_user_pool.app import lambda_handler
from unittest.mock import patch, MagicMock

mock_body = {
    "body":json.dumps({
        "name":"yets",
        "lastname":"panes",
        "email":"omardejesussantanderverdayes01@gmail.com",
        "fk_rol":"613743aa301711efb5fd0affc0d2e18d",
        "fk_subscription":"18b51cb8301a11efb5fd0affc0d2e18d"
    })
}

class TestApp(unittest.TestCase):

    @patch.dict("os.environ", {"DB_USER": "user", "DB_PASSWORD": "password", "DB_NAME": "database", "DB_HOST": "host"})
    @patch("insert_user_pool.app.db_connection.connect")
    def test_lambda_handler(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = [
            {
                "statusCode": 400,
                'body': json.dumps('Usuario registrado, verifica tu correo electronico')
            }
        ]
        result = lambda_handler(mock_body, None)
        self.assertEqual(result["statusCode"], 400)
        body = json.loads(result["body"])

        print(result)