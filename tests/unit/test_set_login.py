from set_login.set_login import lambda_handler
import unittest
import json

mock_body = {
    "body": json.dumps({
        "email": "yexbernal@gmail.com",
        "password": "Yets$123"
    })
}

class MyTestCase(unittest.TestCase):
    def test_lambda_handler(self):
        result = lambda_handler(mock_body, None)
        print(result)


if __name__ == '__main__':
    unittest.main()