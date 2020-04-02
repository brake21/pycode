import requests
import jwt
import time
import random


class StellaWrapper(object):
    DOMAIN = 'xxxx'

    def __init__(self, api_key, secret_key, auth_email):
        self.api_key = api_key
        self.secret_key = secret_key
        self.auth_email = auth_email

    def _generatetoken(self):
        """ Generates a signed JSON Web Token with the Stella secret key
        """
        payload = {

            'Domain': self.DOMAIN,
            'email': self.auth_email
        }
        # sign with secret key
        return jwt.encode(payload, self.secret_key, algorithm='HS256').decode('utf-8')

    def _makeheaders(self):
        """ Generate headers for the APIget request """

        return {
            'Authorization': '{}'.format(self._generatetoken()),
            'content-type': 'application/json',
            'X-API-KEY': '{}'.format(self.api_key),
            'accept': 'application/json'
        }

    def make_api_call(self, url, method='get', api_params=None):
        """ Sends secure request to the Stella API

            Arguments:
                    1.  Method
                    2.  Endpoint
                    3.  Extra parameters
            Kwargs:
            method: HTTP method to send (default GET)
            data: Dictionary of data to send. In case of GET a dictionary
        """

        headers = self._makeheaders()
        max_attempts = 2
        attempts = 0
        while attempts < max_attempts:
            if method == 'get':
                response = requests.get(url, headers=headers, params=api_params)

            else:
                print('Not a get method')
                break
            try:
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                time.sleep((2 ** attempts) + random.random())
                attempts += 1

                if attempts == max_attempts:
                    raise Exception('Error message from Stella: {0}\n'
                                    'Error details: {1}'
                                    .format(e, response.text))
            else:
                break
