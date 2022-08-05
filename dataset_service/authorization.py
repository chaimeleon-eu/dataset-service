from enum import Enum
from dataset_service.POSIX import *
    
class User:
    roles = None

    def __init__(self, token):
        self.token = token    # it is None if unregistered user
            
    def _appendIfNotExists(array, item):
        if item not in array: array.append(item)

    @classmethod
    def validateToken(cls, token, serviceAccount):
        if not "sub" in token.keys(): return False, "sub"
        if not serviceAccount:
            if not "preferred_username" in token.keys(): return False, "preferred_username"
            if not "name" in token.keys(): return False, "name"
            if not "email" in token.keys(): return False, "email"

        try:
            token["appRoles"] = token["resource_access"]["dataset-service"]["roles"]
        except:
            return False, "resource_access.dataset-service.roles"

        # ensure roles included in other roles
        if User.roles.admin_datasets in token["appRoles"]:
            cls._appendIfNotExists(token["appRoles"], User.roles.access_all_datasets)
        if User.roles.superadmin_datasets in token["appRoles"]:
            cls._appendIfNotExists(token["appRoles"], User.roles.access_all_datasets)
            cls._appendIfNotExists(token["appRoles"], User.roles.admin_datasets)
        return True, None


    def isUnregistered(self):
        return self.token is None

    def canCreateDatasets(self):
        return self.token != None and User.roles.admin_datasets in self.token["appRoles"]

    class Access_type(Enum):
        VIEW_DETAILS = 1
        USE = 2

    def canAccessDataset(self, dataset, access_type = Access_type.VIEW_DETAILS):
        if self.token != None and User.roles.superadmin_datasets in self.token["appRoles"]: return True

        if dataset["invalidated"] and access_type is User.Access_type.USE: return False

        if dataset["draft"] and (self.token is None or self.token["sub"] != dataset["authorId"]):
            return False

        if dataset["public"]: return True

        return (self.token != None and User.roles.access_all_datasets in self.token["appRoles"])

    def getEditablePropertiesByTheUser(self, dataset):
        editableProperties = []
        if self.canModifyDataset(dataset):
            if dataset["draft"]: 
                editableProperties.append("draft")
                editableProperties.append("name")
                editableProperties.append("description")
            else:
                editableProperties.append("public")
                editableProperties.append("pids")
            editableProperties.append("invalidated")
            editableProperties.append("contactInfo")
            editableProperties.append("license")
        return editableProperties

    def canModifyDataset(self, dataset):
        if self.token is None: return False
        if not User.roles.admin_datasets in self.token["appRoles"]: return False
        if User.roles.superadmin_datasets in self.token["appRoles"]: return True
        return self.token["sub"] == dataset["authorId"]

    class Search_filter():
        def __init__(self, draft, public, invalidated):
            self.draft = draft
            self.public = public
            self.invalidated = invalidated
            self.userIdForInvalidatedAndDraft = None

    def adjustSearchFilterByUser(self, search_filter):
        if self.token is None or not User.roles.access_all_datasets in self.token["appRoles"]:
            search_filter.public = True
            search_filter.invalidated = False
            search_filter.draft = False
            return search_filter

        if not User.roles.admin_datasets in self.token["appRoles"]:
            search_filter.invalidated = False
            search_filter.draft = False
            return search_filter

        if not User.roles.superadmin_datasets in self.token["appRoles"]:
            search_filter.userIdForInvalidatedAndDraft = self.token["sub"]

        return search_filter

    def userCanAdminUsers(self):
        return self.token != None and self.roles.admin_users in self.token["appRoles"]

    def userCanAdminDatasetAccess(self):
        return self.token != None and self.roles.admin_datasetAccess in self.token["appRoles"]


    def tmpUserCanAccessDataset(userId, userGroups, dataset, access_type = Access_type.USE):
        if "cloud-services-and-security-management" in userGroups: return True

        if dataset["invalidated"] and access_type is User.Access_type.USE: return False

        if dataset["draft"] and userId != dataset["authorId"]:
            return False

        if dataset["public"]: return True
        
        return "data-scientists" in userGroups


