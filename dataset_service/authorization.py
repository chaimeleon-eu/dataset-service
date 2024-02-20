from enum import Enum
from dataset_service.POSIX import *

class Access_type(Enum):
    VIEW_DETAILS = 1
    USE = 2

class User:
    roles = None

    def __init__(self, token: dict | None):
        self._token = token    # it is None if unregistered user

    @classmethod
    def _appendIfNotExists(cls, array, item):
        if item not in array: array.append(item)

    @classmethod
    def validateToken(cls, token, serviceAccount):
        if not "sub" in token.keys(): return False, "sub"
        if not serviceAccount:
            if not "preferred_username" in token.keys(): return False, "preferred_username"
            if not "name" in token.keys(): return False, "name"
            if not "email" in token.keys(): return False, "email"
            if not "groups" in token.keys(): return False, "groups"
        try:
            token["appRoles"] = token["resource_access"]["dataset-service"]["roles"]
        except:
            #Keycloak removes the roles array if the user don't have any role
            #so let's set empty instead of return error
            token["appRoles"] = []
        
        # ensure roles included in other roles
        if User.roles.admin_datasets in token["appRoles"]:
            cls._appendIfNotExists(token["appRoles"], User.roles.access_all_datasets)
        if User.roles.superadmin_datasets in token["appRoles"]:
            cls._appendIfNotExists(token["appRoles"], User.roles.access_all_datasets)
            cls._appendIfNotExists(token["appRoles"], User.roles.admin_datasets)
        return True, None

    @property
    def uid(self):
        if self._token is None: raise Exception("Unregistered user doesn't have uid")
        else: return self._token["sub"]
    @property
    def username(self):
        if self._token is None: raise Exception("Unregistered user doesn't have username")
        else: return self._token["preferred_username"]
    @property
    def name(self):
        if self._token is None: raise Exception("Unregistered user doesn't have name")
        else: return self._token["name"]
    @property
    def email(self):
        if self._token is None: raise Exception("Unregistered user doesn't have email")
        else: return self._token["email"]

    def isUnregistered(self):
        return self._token is None

    def isSuperAdminDatasets(self):
        return self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]
    
    def isOpenChallenge(self):
        return (self._token != None and "DATASETS-OPENCHALLENGE" in self._token["groups"])

    def canCreateDatasets(self):
        return self._token != None and User.roles.admin_datasets in self._token["appRoles"]

    def canRelaunchDatasetCreation(self, dataset):
        return self._token != None and User.roles.superadmin_datasets in self._token["appRoles"] \
            and "creating" in dataset and dataset["creating"]

    def canDeleteDataset(self, dataset):
        if self._token is None: return False
        if not self.canModifyDataset(dataset): return False
        if User.roles.superadmin_datasets in self._token["appRoles"]: return True
        return "creating" in dataset and dataset["creating"]

    def canAccessDataset(self, dataset, access_type = Access_type.VIEW_DETAILS):
        if access_type == Access_type.VIEW_DETAILS:
            return self.canViewDatasetDetails(dataset)
        else:   # access_type == Access_type.USE
            return self.canUseDataset(dataset)

    def canViewDatasetDetails(self, dataset):
        if self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]: return True
        if dataset["draft"] and (self._token is None or self._token["sub"] != dataset["authorId"]): return False
        if dataset["public"]: return True
        return (self._token != None and User.roles.access_all_datasets in self._token["appRoles"])

    def canUseDataset(self, dataset):
        if not self.canViewDatasetDetails: return False

        if dataset["draft"] and dataset["creating"]: return False
        if self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]: return True
        if dataset["invalidated"]: return False
        if dataset["public"]: return (self._token != None)
        return (self._token != None and "data-scientists" in self._token["groups"])

    def canCheckIntegrityOfDatasets(self):
        return self.isSuperAdminDatasets()

    def getEditablePropertiesByTheUser(self, dataset):
        editableProperties = []
        if self.canModifyDataset(dataset):
            if dataset["draft"]: 
                if not dataset["creating"]:
                    editableProperties.append("draft")
                editableProperties.append("name")
                editableProperties.append("description")
                editableProperties.append("previousId")
#           else:
#               editableProperties.append("public")
#               editableProperties.append("pids")
            editableProperties.append("invalidated")
            editableProperties.append("contactInfo")
            editableProperties.append("license")
        return editableProperties
    
    def getAllowedActionsForTheUser(self, dataset):
        allowedActions = []
        if self.canAccessDataset(dataset, Access_type.USE):
            allowedActions.append("use")
        if self.canDeleteDataset(dataset):
            allowedActions.append("delete")
        if self.canCheckIntegrityOfDatasets():
            allowedActions.append("checkIntegrity")
        if self.canRelaunchDatasetCreation(dataset):
            allowedActions.append("relaunchCreationJob")
        if self.canAdminDatasetAccesses():
            allowedActions.append("viewAccessHistory")
        return allowedActions

    def canModifyDataset(self, dataset):
        if self._token is None: return False
        if not User.roles.admin_datasets in self._token["appRoles"]: return False
        if User.roles.superadmin_datasets in self._token["appRoles"]: return True
        return self._token["sub"] == dataset["authorId"]

    def canAdminUsers(self):
        return self._token != None and self.roles.admin_users in self._token["appRoles"]

    def canAdminDatasetAccesses(self):
        return self._token != None and self.roles.admin_datasetAccess in self._token["appRoles"]


class Search_filter():
    def __init__(self, draft, public, invalidated):
        self.draft = draft
        self.public = public
        self.invalidated = invalidated
        self._userId = None   # for filter invalidated and draft datasets,
                                # if normal user only the author can see them

    def getUserId(self):
        return self._userId

    def adjustByUser(self, user: User):
        if user._token is None or not User.roles.access_all_datasets in user._token["appRoles"]:
            self.public = True
            self.invalidated = False
            self.draft = False
            return

        if not User.roles.admin_datasets in user._token["appRoles"]:
            self.invalidated = False
            self.draft = False
            return

        if not User.roles.superadmin_datasets in user._token["appRoles"]:
            self._userId = user._token["sub"]

class Upgradables_filter():
    def __init__(self):
        self._userId = None

    def getUserId(self):
        return self._userId

    def adjustByUser(self, user: User):
        if user._token is None: raise Exception()
        if not User.roles.superadmin_datasets in user._token["appRoles"]:
            self._userId = user._token["sub"]
