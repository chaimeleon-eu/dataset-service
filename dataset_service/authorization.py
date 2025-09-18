import logging
import json
from dataset_service.POSIX import *

class Roles:
    def __init__(self, roles: dict | None):
        if roles is None: return
        self.use_datasets = roles["use_datasets"]
        self.admin_datasets = roles["admin_datasets"]
        self.superadmin_datasets = roles["superadmin_datasets"]
        self.admin_users = roles["admin_users"]
        self.admin_datasetAccess = roles["admin_datasetAccess"]
        self.admin_projects = roles["admin_projects"]

class User:
    roles = Roles(None)
    client_id = ""
    PROJECT_GROUP_PREFIX = "PROJECT-"
    PROJECT_ADMINS_GROUP_PREFIX = "ADMINS-PROJECT-"

    def __init__(self, token: dict | None):
        self._token = token    # it is None if unregistered user

    @classmethod
    def _appendIfNotExists(cls, array, item):
        if item not in array: array.append(item)

    @classmethod
    def validateToken(cls, token, serviceAccount):
        if getattr(User.roles, 'use_datasets', None) is None: 
            raise Exception("Please set User.roles before trying to validate a token.")
        if User.client_id == "": 
            raise Exception("Please set User.client_id before trying to validate a token.")
        if not "sub" in token.keys(): return False, "sub"
        if not serviceAccount:
            if not "preferred_username" in token.keys(): return False, "preferred_username"
            if not "name" in token.keys(): return False, "name"
            if not "email" in token.keys(): return False, "email"
            #if not "groups" in token.keys(): return False, "groups"   keycloak does not include groups if empty, but still valid token
        try:
            token["appRoles"] = token["resource_access"][User.client_id]["roles"]
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

    def canCreateDatasets(self):
        return self._token != None and User.roles.admin_datasets in self._token["appRoles"]
    
    def canCreateExternalDatasets(self):
        return self.isSuperAdminDatasets()

    def canRestartCreationOfDataset(self, dataset):
        return self.isSuperAdminDatasets() and "creating" in dataset and dataset["creating"]
    
    def canReadjustFilePermissionsOfDatasets(self):
        return self.isSuperAdminDatasets()
    
    def canRecollectMetadataOfDatasets(self):
        return self.isSuperAdminDatasets()

    def canDeleteDataset(self, dataset):
        if self._token is None: return False
        if not self.canModifyDataset(dataset): return False
        if User.roles.superadmin_datasets in self._token["appRoles"]: return True
        return "creating" in dataset and dataset["creating"]

    def canViewDatasetDetails(self, dataset):
        # superadmin always can view
        if self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]: return True
        # when draft only author can view
        if dataset["draft"] and (self._token is None or self._token["sub"] != dataset["authorId"]): return False
        # when public anybody can view
        if dataset["public"]: return True
        # otherwise only users from dataset's project
        return (dataset["project"] in self.getProjects())
    
    def canViewDatasetExtraDetails(self, datasetProject):
        # superadmin always can view
        if self._token != None and User.roles.superadmin_datasets in self._token["appRoles"]: return True
        # otherwise only if the user is in dataset's project can see the dataset's extra details
        return (datasetProject in self.getProjects())   

    def canUseDataset(self, dataset, datasetACL):
        # Essential conditions
        if self._token is None or not User.roles.use_datasets in self._token["appRoles"]: return False
        if not self.canViewDatasetDetails(dataset): return False
        # Special cases
        if dataset["draft"] and dataset["creating"]: return False
        if User.roles.superadmin_datasets in self._token["appRoles"]: return True
        if dataset["invalidated"]: return False
        # The main rule
        userProjects = self.getProjects()
        logging.root.debug("User projects: " + json.dumps(list(userProjects)))
        if dataset["project"] in userProjects: return True
        if dataset["public"]: return (self.uid in datasetACL)
        else: return False

    def canCheckIntegrityOfDatasets(self):
        return self.isSuperAdminDatasets()

    def getAllowedActionsOnDatasetsForTheUser(self):
        allowedActions = []
        if self.canCreateDatasets():
            allowedActions.append("create")
        return allowedActions
    
    def getEditablePropertiesByTheUser(self, dataset):
        editableProperties = []
        if self.canModifyDataset(dataset):
            if dataset["draft"]: 
                if not dataset["creating"]:
                    editableProperties.append("draft")
                editableProperties.extend(["name", "version", "previousId",
                                           "description", "provenance", "purpose",
                                           "type", "collectionMethod"])
                # editableProperties.append("project")
            else:
                if self.isSuperAdminDatasets():
                    editableProperties.append("public")
                    editableProperties.append("pids")
            editableProperties.append("invalidated")
            if dataset["invalidated"]:
                editableProperties.append("invalidationReason")
            editableProperties.extend(["contactInfo", "license"])
            if self.isSuperAdminDatasets():
                editableProperties.append("authorId")
                editableProperties.append("tags")
        return editableProperties
    
    def getAllowedActionsForTheUser(self, dataset, datasetACL):
        allowedActions = []
        if self.canUseDataset(dataset, datasetACL):
            allowedActions.append("use")
        if self.canDeleteDataset(dataset):
            allowedActions.append("delete")
        if self.canCheckIntegrityOfDatasets():
            allowedActions.append("checkIntegrity")
        if self.canRestartCreationOfDataset(dataset):
            allowedActions.append("restartCreation")
        if self.canReadjustFilePermissionsOfDatasets():
            allowedActions.append("readjustFilePermissions")
        if self.canRecollectMetadataOfDatasets():
            allowedActions.append("recollectMetadata")
        if self.canAdminDatasetAccesses():
            allowedActions.append("viewAccessHistory")
        if self.canManageACL(dataset):
            allowedActions.append("manageACL")
        return allowedActions
    
    def getProjects(self) -> set[str]:
        projects = set()
        if self._token is None or not "groups" in self._token.keys(): return projects
        prefix_len = len(User.PROJECT_GROUP_PREFIX)
        for g in self._token["groups"]:
            if g.startswith(User.PROJECT_GROUP_PREFIX):
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
        if self._token is None: return False
        #if not PROJECT_GROUP_PREFIX + dataset["project"] in self._token["groups"]: return False
        return self.roles.admin_datasetAccess in self._token["appRoles"]
    
    def canManageACL(self, dataset):
        return self.canModifyDataset(dataset) or self.canAdminDatasetAccesses()

    def canAdminProjects(self):
        if self._token is None: return False
        return User.roles.admin_projects in self._token["appRoles"]
    
    def canModifyProject(self, projectCode: str):
        if self.canAdminProjects(): return True
        if self._token is None or not "groups" in self._token.keys(): return False
        prefix_len = len(User.PROJECT_ADMINS_GROUP_PREFIX)
        for g in self._token["groups"]:
            if g.startswith(User.PROJECT_ADMINS_GROUP_PREFIX) and g[prefix_len:] == projectCode: return True
        return False
    
    def getAllowedActionsOnProjectsForTheUser(self):
        allowedActions = []
        if self.canAdminProjects():
            allowedActions.append("create")
        return allowedActions
    
    def getEditablePropertiesOfProjectByTheUser(self, projectCode: str):
        editableProperties = []
        if self.canModifyProject(projectCode):
            editableProperties.append("name")
            editableProperties.append("shortDescription")
            editableProperties.append("externalUrl")
            editableProperties.append("logoUrl")
        return editableProperties
    
    def getAllowedActionsOnProjectForTheUser(self, projectCode: str):
        allowedActions = []
        if self.canModifyProject(projectCode):
            allowedActions.append("config")
            allowedActions.append("viewSubprojects")
        # if self.canDeleteProject(projectCode):
        #     allowedActions.append("delete")
        # if self.canManageMembers(projectCode):
        #     allowedActions.append("manageMembers")
        return allowedActions
    
    def getAllowedActionsOnSubprojectsForTheUser(self,  projectCode: str):
        allowedActions = []
        if self.canAdminProjects():
            allowedActions.append("create")
            allowedActions.append("edit")
        # if self.canDeleteProject(projectCode):
        #     allowedActions.append("delete")
        return allowedActions


class Search_filter():
    def __init__(self, draft: bool | None = None, public: bool | None = None, 
                 invalidated: bool | None = None, projects: set[str] | None = None):
        self.draft = draft
        self.public = public
        self.invalidated = invalidated
        self.tags = set()
        # projects for filter public datasets
        self._projectsForPublic = projects.copy() if projects != None else None
        # projects for filter non-public datasets
        self._projectsForNonPublic = projects.copy() if projects != None else None
        self._userId = None   # For filter invalidated and draft datasets,
                              # normal user only can see them if he/she is the author.
    
    def setSelectedProjects(self, projects: set[str] | None):
        ''' Set selected projects to filter '''
        self._projectsForPublic = projects.copy() if projects != None else None
        self._projectsForNonPublic = projects.copy() if projects != None else None
    
    def getUserId(self):
        return self._userId

    def getProjectsForPublic(self):
        return self._projectsForPublic

    def getProjectsForNonPublic(self):
        return self._projectsForNonPublic

    def adjustByUser(self, user: User):
        self._userId = None

        if user._token is None:   # unregistered user
            # self.public = True
            self.invalidated = False
            self.draft = False
            self._projectsForNonPublic = set()  # empty: that user can't see non-public datasets
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
