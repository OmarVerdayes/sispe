from set_password.set_password import lambda_handler
import unittest
import json

mock_body = {
    "body": json.dumps({
        "email": "20213tn042@utez.edu.mx",
        "temporary_password": "H7QeIq?z",
        "new_password": "Yets$123"
    })
}

class MyTestCase(unittest.TestCase):
    def test_lambda_handler(self):
        result = lambda_handler(mock_body, None)
        print(result)


if __name__ == '__main__':
    unittest.main()