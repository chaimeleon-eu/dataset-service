from datetime import datetime, timezone
import logging
import urllib.parse
import urllib.error
import http.client
import json
import jwt

class LoginException(Exception):
    pass
    
class AuthClient:
    TOKEN_EXPIRATION_LEEWAY = 60  # seconds

    def __init__(self, oidc_url, client_id, client_secret):
        self._oidc_url = oidc_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._token = None
        self._exp = None

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
        if self._token != None:
            if self._exp is None:
                decodedToken = jwt.decode(self._token, options={'verify_signature': False})
                self._exp = int(decodedToken["exp"])
            now = datetime.now(tz=timezone.utc).timestamp()
            if now > (self._exp - self.TOKEN_EXPIRATION_LEEWAY):    # token expired or few time left
                self._token = None
                self._exp = None
        if self._token is None:
            self._token = self._login()
        return self._token
