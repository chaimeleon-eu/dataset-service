import logging
import urllib.parse
import urllib.error
import http.client
import json
import time
from datetime import datetime
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
    
    def _POST_JSON(self, path, content):
        connection = self._get_connection()
        headers = self._get_headers()
        headers['Content-Type'] = 'application/json'
        try:
            connection.request("POST", self.apiURL.path + path, content, headers)
            res = connection.getresponse()
            httpStatusCode = res.status
        finally:
            connection.close()
        if httpStatusCode != 201:
            logging.root.error('KeycloakAdminAPI error. Code: %d %s' % (httpStatusCode, res.reason))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI call failed.', httpStatusCode)
        logging.root.debug('KeycloakAdminAPI call success.')
    
    def _DELETE_JSON(self, path, content):
        connection = self._get_connection()
        headers = self._get_headers()
        headers['Content-Type'] = 'application/json'
        try:
            connection.request("DELETE", self.apiURL.path + path, content, headers)
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

    def _get_subgroups(self, kcGroups: list, byPath: bool, recursive: bool = True) -> list[str]:
        groups = []
        for group in kcGroups:
            newItem = group["path"] if byPath else group["name"]
            groups.append(newItem)
            if recursive and group["subGroupCount"] > 0:  # It is a group of groups
                response = self._GET_JSON("groups/"+group["id"]+"/children")
                groups.extend(self._get_subgroups(response, byPath))
        return groups

    def getGroups(self, userID: str = '', byPath: bool = True, recursive: bool = True):
        logging.root.debug('Getting groups from KeycloakAdminAPI...')
        try:
            response = self._GET_JSON("groups")
            return self._get_subgroups(response, byPath)
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')
    
    def getUserGroups(self, userID: str, byPath: bool = True):
        logging.root.debug('Getting user groups from KeycloakAdminAPI...')
        response = self._GET_JSON("users/"+userID+"/groups")
        groups = []
        try:
            for group in response:
                newItem = group["path"] if byPath else group["name"]
                groups.append(newItem)
            return groups
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')
    
    def _get_child_group_id(self, child_name: str, parent_id: str = 'root') -> str | None:
        url = "groups" if parent_id == 'root' else "groups/"+parent_id+"/children"
        url += ("?search="+child_name)
        response = self._GET_JSON(url)
        for group in response:
            if group["name"] == child_name:
                return group["id"]
        return None

    def _get_group_id(self, path: str) -> str | None:
        logging.root.debug('Getting group id from KeycloakAdminAPI...')
        path = path[1:]
        levels = path.split('/')
        previous = 'root'
        for l in levels:
            id = self._get_child_group_id(l, previous)
            if id is None: return None
            previous = id
        return id

    def addUserToGroup(self, userId: str, groupPath: str):
        logging.root.debug('Searching group id with KeycloakAdminAPI...')
        groupId = self._get_group_id(groupPath)
        if groupId is None: 
            logging.root.error("KeycloakAdminAPI error: group path '%s' not found" % (groupPath))
            raise KeycloakAdminAPIException('Internal server error: group path not found.')
        
        logging.root.debug('Adding user to group with KeycloakAdminAPI...')
        self._PUT_JSON("users/"+userId+"/groups/"+groupId, "{}")
    
    def removeUserFromGroup(self, userId: str, groupPath: str):
        logging.root.debug('Searching group id with KeycloakAdminAPI...')
        groupId = self._get_group_id(groupPath)
        if groupId is None: 
            logging.root.error("KeycloakAdminAPI error: group path '%s' not found" % (groupPath))
            raise KeycloakAdminAPIException('Internal server error: group path not found.')
        
        logging.root.debug('Removing user from group with KeycloakAdminAPI...')
        self._DELETE_JSON("users/"+userId+"/groups/"+groupId, "{}")
    

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
    
    DEFAULT_ATTRIBUTES_GROUP = "base-attributes"

    def getUserAttributes(self, userId) -> list[dict]:
        logging.root.debug('Getting user from KeycloakAdminAPI...')
        user = self._GET_JSON("users/"+userId+"?userProfileMetadata=true")
        attributeGroups = {}
        for attGroup in user["userProfileMetadata"]["groups"]:
            attributeGroups[attGroup["name"]] = {
                "displayName": attGroup["displayHeader"],
                "attributes": []
            }
        # Add a default group for the basic attributes
        if not self.DEFAULT_ATTRIBUTES_GROUP in attributeGroups:
            attributeGroups[self.DEFAULT_ATTRIBUTES_GROUP] = {
                "displayName": "Base attributes",
                "attributes": []
            }
        for attProfile in user["userProfileMetadata"]["attributes"]:
            attName = attProfile["name"]
            attDisplayName = attProfile["displayName"]
            attValues = ""
            if "attributes" in user and attName in user["attributes"]:
                attValues = user["attributes"][attName]
            elif attName in user:  # base attribute
                attValues = user[attName]
            attGroup = attProfile["group"] if "group" in attProfile else self.DEFAULT_ATTRIBUTES_GROUP
            attributeGroups[attGroup]["attributes"].append({
                "displayName": attDisplayName,
                "values": attValues
            })
        return list(attributeGroups.values())

    def getUserAttribute(self, userId, attributeName):
        logging.root.debug('Getting user from KeycloakAdminAPI...')
        user = self._GET_JSON("users/"+userId)
        if "attributes" in user and attributeName in user["attributes"]:
            return user["attributes"][attributeName][0] # If the attribute is repeated there will be more than one items, 
                                                        # but let's return only the first value.
        else: return None

    def setUserAttribute(self, userId, attributeName, attributeValue):
        logging.root.debug('Getting user from KeycloakAdminAPI...')
        user = self._GET_JSON("users/"+userId)
        if not "attributes" in user: user["attributes"] = {}
        if not attributeName in user["attributes"]:
            attributeValues = [attributeValue]
        else:
            attributeValues = user["attributes"][attributeName]
            attributeValues[0] = attributeValue
        user["attributes"][attributeName] = attributeValues
        logging.root.debug('Setting user attribute with KeycloakAdminAPI...')
        self._PUT_JSON("users/"+userId, json.dumps(user))

    def getUsers(self, skip: int = 0, limit: int = 0, searchString: str = '', disabled: bool | None = None):
        logging.root.debug('Getting users from KeycloakAdminAPI...')
        firstParam = "" if skip == 0 else "&first="+str(skip)
        maxParam = "" if limit == 0 else "&max="+str(limit)
        searchStringParam = "" if searchString == "" else "&search="+searchString
        enabledParam = "" if disabled is None else "&enabled="+str(not disabled)
        try:
            total = self._GET_JSON("users/count?"+searchStringParam+enabledParam)
            response = self._GET_JSON("users?briefRepresentation=false"+firstParam+maxParam+searchStringParam+enabledParam)
            users = []
            for item in response:
                users.append({
                    "uid": item["id"],
                    "username": item["username"],
                    "email": item["email"] if "email" in item else "",
                    "name": item["firstName"] + " " + item["lastName"] if "firstName" in item else "",
                    "creationDate": str(datetime.fromtimestamp(item["createdTimestamp"]/1000).astimezone()),
                    "disabled": not item["enabled"],
                    "emailVerified": item["emailVerified"]
                })
            return users, total
        except (Exception) as e:
            logging.root.error('KeycloakAdminAPI response unexpected: %s' % (response))
            raise KeycloakAdminAPIException('Internal server error: KeycloakAdminAPI response unexpected.')

    def createGroup(self, name, parentGroupPath: str):
        parentGroupId = self._get_group_id(parentGroupPath)
        if parentGroupId is None: 
            logging.root.error("KeycloakAdminAPI error: group path '%s' not found" % (parentGroupId))
            raise KeycloakAdminAPIException('Internal server error: parent group path not found.')
        logging.root.debug('Creating group in KeycloakAdminAPI...')
        self._POST_JSON("groups/"+parentGroupId+"/children", json.dumps({"name": name}))
