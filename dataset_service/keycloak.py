import logging
import urllib.parse
import urllib.error
import http.client
import json
import time
from dataset_service import auth

class KeycloakAdminAPIException(Exception):
    def __init__(self, message: str, error_code: int = 0):
        super().__init__(message)
        self.error_code = error_code
    
class KeycloakAdminAPIClient:
    def __init__(self, authClient: auth.AuthClient, apiURL: str, clientIdForGetUserTokens: str):
        self.apiURL = urllib.parse.urlparse(apiURL)
        if self.apiURL.hostname is None: raise Exception('Wrong apiUrl.')
        self.authClient = authClient
        self.clientUIDForGetUserTokens = self.check_connection_getting_the_clientUIDForGetUserTokens(clientIdForGetUserTokens)
        

    def check_connection_getting_the_clientUIDForGetUserTokens(self, clientIdForGetUserTokens: str) -> str:
        logging.root.info("Checking connection to KeycloakAdminAPI on %s..." % self.apiURL.geturl())
        try:
            retries = 0
            while retries < 5:
                try:
                    clientUIDForGetUserTokens = self.getClientUID(clientIdForGetUserTokens)
                    if clientUIDForGetUserTokens is None: raise KeycloakAdminAPIException("Wrong clientIdForGetUserToken", 404)
                    else:
                        logging.root.info("Client UID for getting user tokens: %s" % clientUIDForGetUserTokens)
                        return clientUIDForGetUserTokens
                except urllib.error.HTTPError as e1:
                    logging.root.error("HTTPError: " + str(e1.code) + " - " + str(e1.reason))
                except urllib.error.URLError as e2:
                    logging.root.error("URLError: "+ str(e2.reason))
                retries += 1
                logging.root.info("Retrying in 5 seconds...")
                time.sleep(5)
        except (KeycloakAdminAPIException) as e:
            if e.error_code == 403:
                logging.root.error('''
                    Forbidden access to Keycloak Admin API. 
                    Please give the proper roles to the service account 
                    (see comments for the auth.admin_api_url in the default configuration file).''')
            raise e
        except Exception as e:
            logging.root.exception(e)
            raise e
        raise Exception("Unable to connect to KeycloakAdminAPI.")

    def _GET_JSON(self, path):
        if self.apiURL.hostname is None: raise Exception('Wrong apiUrl.')
        connection = http.client.HTTPSConnection(self.apiURL.hostname, self.apiURL.port)
        headers = {}
        headers['Authorization'] = 'bearer ' + self.authClient.get_token()
        payload = ""
        try:
            connection.request("GET", self.apiURL.path + path, payload, headers)
            res = connection.getresponse()
            httpStatusCode = res.status
            msg = res.read()  # whole response must be readed in order to do more requests using the same connection
        finally:
            connection.close()
        if httpStatusCode != 200:
            logging.root.error('KeycloakAdminAPI error. Code: %d %s' % (httpStatusCode, res.reason))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI call failed.', httpStatusCode)
        logging.root.debug('KeycloakAdminAPI call success.')
        return json.loads(msg)

    def getClientUID(self, clientId: str) -> str | None:
        logging.root.debug('Getting client UID for clientId "%s" from KeycloakAdminAPI...' % clientId)
        response = self._GET_JSON("clients?clientId="+urllib.parse.quote_plus(clientId))
        try:
            if len(response) == 0: return None
            if len(response) != 1: raise Exception("Unexpected response, clientId not unique")
            client = response[0]
            if client["clientId"] != clientId: raise Exception("Unexpected response, clientId not match")
            return client["id"]
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')

    def getGroups(self):
        logging.root.debug('Getting groups from KeycloakAdminAPI...')
        response = self._GET_JSON("groups")
        try:
            groups = []
            for group in response:
                groups.append(group["name"])
            return groups
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: unexpected KeycloakAdminAPI response.')

    def getUserGroups(self, userUID):
        logging.root.debug('Getting user groups from KeycloakAdminAPI...')
        response = self._GET_JSON("users/"+userUID+"/groups")
        groups = []
        try:
            for group in response:
                groups.append(group["name"])
            return groups
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')

    def getUserId(self, username):
        logging.root.debug('Getting user ID from KeycloakAdminAPI...')
        response = self._GET_JSON("users?exact=true&briefRepresentation=true&username="+urllib.parse.quote_plus(username))
        try:
            if len(response) == 0: return None
            if len(response) != 1: raise Exception("Unexpected response, username not unique")
            user = response[0]
            if user["username"] != username: raise Exception("Unexpected response, username not match")
            return user["id"]
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')

    def getUserToken(self, userUID) -> dict:
        # Possible alternative method: impersonation
        logging.root.debug('Getting user token from KeycloakAdminAPI...')
        return self._GET_JSON(
            "clients/"+self.clientUIDForGetUserTokens+"/evaluate-scopes/generate-example-access-token?scope=openid&userId="+userUID)

