import logging
import urllib.parse
import urllib.error
import http.client
import json

class LoginException(Exception):
    pass
    
class AuthClient:

    def __init__(self, oidc_url, client_id, client_secret):
        self._oidc_url = oidc_url
        self._client_id = client_id
        self._client_secret = client_secret

    def _login(self):
        logging.root.debug("Logging into the auth service...")
        auth = urllib.parse.urlparse(self._oidc_url)
        if auth.hostname is None: raise Exception('Wrong oidc_url.')
        connection = http.client.HTTPSConnection(auth.hostname, auth.port)

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = urllib.parse.urlencode({'client_id' : self._client_id, 'client_secret' : self._client_secret, 'grant_type': 'client_credentials'})
        connection.request("POST", auth.path, payload, headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()  # whole response must be readed in order to do more requests using the same connection
        if httpStatusCode != 200:
            logging.root.error('Auth login error. Code: %d %s' % (httpStatusCode, res.reason))
            raise LoginException('Internal server error: Auth login failed.')
        else:
            logging.root.debug('Auth login success.')
            response = json.loads(msg)
            #print(response)
            return response['access_token']

    def get_token(self):
        return self._login()
