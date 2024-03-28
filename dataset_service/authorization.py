from enum import Enum
from dataset_service.POSIX import *

class Access_type(Enum):
    VIEW_DETAILS = 1
    USE = 2

PROJECT_GROUP_PREFIX = "PROJECT-"

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
            cls._appendIfNotExists(token["appRoles"], User.roles.use_datasets)
        if User.roles.superadmin_datasets in token["appRoles"]:
            cls._appendIfNotExists(token["appRoles"], User.roles.use_datasets)
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
        return (self._token != None and PROJECT_GROUP_PREFIX + dataset["project"] in self._token["groups"])

    def canUseDataset(self, dataset):
        if not self.canViewDatasetDetails: return False

        if dataset["draft"] and dataset["creating"]: return False
        if self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]: return True
        if dataset["invalidated"]: return False
        if dataset["public"]: return (self._token != None)
        return (self._token != None and User.roles.use_datasets in self._token["appRoles"])

    def canCheckIntegrityOfDatasets(self):
        return self.isSuperAdminDatasets()

    def getEditablePropertiesByTheUser(self, dataset):
        editableProperties = []
        if self.canModifyDataset(dataset):
            if dataset["draft"]: 
                if not dataset["creating"]:
                    editableProperties.append("draft")
                editableProperties.append("name")
                # editableProperties.append("project")
                editableProperties.append("description")
                editableProperties.append("previousId")
            else:
                if self.isSuperAdminDatasets():
                    editableProperties.append("public")
                    editableProperties.append("pids")
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
    
    def getProjects(self) -> set[str]:
        projects = set()
        if self._token is None: return projects
        prefix_len = len(PROJECT_GROUP_PREFIX)
        for g in self._token["groups"]:
            if g.startswith(PROJECT_GROUP_PREFIX):
                projects.add(g[prefix_len:])
        return projects

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
    def __init__(self, draft: bool | None = None, public: bool | None = None, 
                 invalidated: bool | None = None, projects: set[str] | None = None):
        self.draft = draft
        self.public = public
        self.invalidated = invalidated
        self._projectsForPublic = projects  # projects for filter public datasets
        self._projectsForNonPublic = projects  # project for filter non-public datasets
        self._userId = None   # For filter invalidated and draft datasets,
                              # normal user only can see them if he/she is the author.
    
    def setSelectedProjects(self, projects: set[str] | None):
        ''' Set selected projects to filter '''
        self._projectsForPublic = projects
        self._projectsForNonPublic = projects
    
    def getUserId(self):
        return self._userId

    def getProjectsForPublic(self):
        return self._projectsForPublic

    def getProjectsForNonPublic(self):
        return self._projectsForNonPublic

    def adjustByUser(self, user: User):
        self._userId = None

        if user._token is None:   # unregistered user
            self.public = True
            self.invalidated = False
            self.draft = False
            self._projectsForNonPublic = set()
            return
        
        if not User.roles.superadmin_datasets in user._token["appRoles"]:
            # non-superadmin user only can see non-public datasets of projects which he/she is joined to
            user_projects = user.getProjects()
            if self._projectsForNonPublic is None: 
                self._projectsForNonPublic = user_projects 
            else: 
                self._projectsForNonPublic.intersection_update(user_projects)

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
