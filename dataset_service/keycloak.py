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
    def __init__(self, authClient: auth.AuthClient, apiURL: str):
        self.apiURL = urllib.parse.urlparse(apiURL)
        if self.apiURL.hostname is None: raise Exception('Wrong apiUrl.')
        self.authClient = authClient

    def check_connection(self):
        logging.root.info("Checking connection to KeycloakAdminAPI on %s..." % self.apiURL.geturl())
        try:
            retries = 0
            while retries < 5:
                try:
                    groups = self.getGroups()
                    logging.root.info("Defined groups: %s" % json.dumps(groups))
                    break
                except urllib.error.HTTPError as e1:
                    logging.root.error("HTTPError: " + str(e1.code) + " - " + str(e1.reason))
                except urllib.error.URLError as e2:
                    logging.root.error("URLError: "+ str(e2.reason))
                retries += 1
                logging.root.info("Retrying in 5 seconds...")
                time.sleep(5)
            if retries >= 5: raise Exception("Unable to connect to KeycloakAdminAPI.")
        except (KeycloakAdminAPIException) as e:
            if e.error_code == 403:
                logging.root.error('''
                    Forbidden access to Keycloak Admin API. 
                    Please give the proper roles to the service account 
                    (see comments for the auth.admin_api_url in the default configuration file).''')
            raise(e)
        except Exception as e:
            logging.root.exception(e)
            raise e

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

