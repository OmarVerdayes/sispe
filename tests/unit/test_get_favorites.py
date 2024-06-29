from unittest.mock import patch, MagicMock
import unittest
from botocore.exceptions import ClientError
import json
from get_favorites.get_favorites import lambda_handler

mock_event = {
    "pathParameters": {
        "fk_user": "qwertyuiop1234567890abcdef1234567890"
    }
}

class MyTestCase(unittest.TestCase):

    @patch.dict("os.environ", {"DB_USER": "user", "DB_PASSWORD": "password", "DB_NAME": "database", "DB_HOST": "host"})
    @patch("get_favorites.db_connection.connect")
    def test_lambda_handler(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock the SQL execution and results
        mock_conn.execute.return_value.fetchall.return_value = [
            {
                "fk_film": bytes.fromhex("1234567890abcdef1234567890abcdef"),
                "title": "Film Title",
                "description": "Film Description",
                "length": 120,
                "category_name": "Category"
            }
        ]

        result = lambda_handler(mock_event, None)
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertTrue(isinstance(body, list))
        self.assertEqual(body[0]["title"], "Film Title")

        mock_conn.close.assert_called_once()

    @patch.dict("os.environ", {"DB_USER": "user", "DB_PASSWORD": "password", "DB_NAME": "database", "DB_HOST": "host"})
    @patch("get_favorites.db_connection.connect")
    def test_lambda_handler_no_favorites(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock the SQL execution and results
        mock_conn.execute.return_value.fetchall.return_value = []

        result = lambda_handler(mock_event, None)
        mock_conn.close.assert_called_once()
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"]), 'Favoritos no encontrados')

    @patch.dict("os.environ", {"DB_USER": "user", "DB_PASSWORD": "password", "DB_NAME": "database", "DB_HOST": "host"})
    @patch("get_favorites.db_connection.connect")
    def test_lambda_handler_user_not_found(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock the SQL execution and results
        mock_conn.execute.side_effect = [None, []]

        result = lambda_handler(mock_event, None)
        self.assertEqual(result["statusCode"], 400)
        self.assertEqual(json.loads(result["body"]), 'Usuario no encontrado')

if __name__ == '__main__':
    unittest.main()
