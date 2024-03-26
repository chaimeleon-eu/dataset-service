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

#API SPEC: https://www.keycloak.org/docs-api/22.0.5/rest-api/index.html#_users

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

    def _get_connection(self):
        if self.apiURL.hostname is None: raise Exception('Wrong apiUrl.')
        return http.client.HTTPSConnection(self.apiURL.hostname, self.apiURL.port)
    def _get_headers(self):
        headers = {}
        headers['Authorization'] = 'bearer ' + self.authClient.get_token()
        return headers

    def _GET_JSON(self, path):
        connection = self._get_connection()
        try:
            connection.request("GET", self.apiURL.path + path, body="", headers=self._get_headers())
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

    def _PUT_JSON(self, path, content):
        connection = self._get_connection()
        headers = self._get_headers()
        headers['Content-Type'] = 'application/json'
        try:
            connection.request("PUT", self.apiURL.path + path, content, headers)
            res = connection.getresponse()
            httpStatusCode = res.status
        finally:
            connection.close()
        if httpStatusCode != 204:
            logging.root.error('KeycloakAdminAPI error. Code: %d %s' % (httpStatusCode, res.reason))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI call failed.', httpStatusCode)
        logging.root.debug('KeycloakAdminAPI call success.')

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

    def getGroups(self, subgroupsOfTheGroup: str = ''):
        if subgroupsOfTheGroup == '':
            logging.root.debug('Getting groups from KeycloakAdminAPI...')
        else: logging.root.debug('Getting subgroups of "%s" from KeycloakAdminAPI...' % subgroupsOfTheGroup)
        response = self._GET_JSON("groups")
        try:
            if subgroupsOfTheGroup == '':
                responseGroups = response 
            else:
                responseGroups = []
                for group in responseGroups:
                    if group["name"] == subgroupsOfTheGroup:
                        responseGroups = group["subGroups"]
            groups = []
            for group in responseGroups:
                groups.append(group["name"])
            return groups
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: unexpected KeycloakAdminAPI response.')

    def getUserGroups(self, userID):
        logging.root.debug('Getting user groups from KeycloakAdminAPI...')
        response = self._GET_JSON("users/"+userID+"/groups")
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

    def getUserToken(self, userID) -> dict:
        logging.root.debug('Getting user token from KeycloakAdminAPI...')
        return self._GET_JSON(
            "clients/"+self.clientUIDForGetUserTokens+"/evaluate-scopes/generate-example-access-token?scope=openid&userId="+userID)
        # Possible alternative method: impersonation
        # Other alternative method without using that keycloakAdminAPI: use token exchange
        # https://www.keycloak.org/docs/latest/securing_apps/index.html#direct-naked-impersonation
        # Indeed you will need to use one of those alternatives if you need a signed and coded token.

    def getUserAttribute(self, userId, attributeName):
        logging.root.debug('Getting user from KeycloakAdminAPI...')
        user = self._GET_JSON("users/"+userId)
        if attributeName in user["attributes"]:
            return user["attributes"][attributeName][0] # If the attribute is repeated there will be more than on items, 
                                                        # but let's return only the first value.
        else: return None

    def setUserAttribute(self, userId, attributeName, attributeValue):
        logging.root.debug('Getting user from KeycloakAdminAPI...')
        user = self._GET_JSON("users/"+userId)
        if not attributeName in user["attributes"]:
            attributeValues = [attributeValue]
        else:
            attributeValues = user["attributes"][attributeName]
            attributeValues[0] = attributeValue
        user["attributes"][attributeName] = attributeValues
        logging.root.debug('Setting user attribute with KeycloakAdminAPI...')
        self._PUT_JSON("users/"+userId, json.dumps(user))
