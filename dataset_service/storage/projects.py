from .DB import DB

class DBProjectsOperator():
    def __init__(self, db: DB):
        self.cursor = db.cursor

    def createOrUpdateProject(self, code, name, shortDescription, externalUrl, logoFileName):
        self.cursor.execute("""
            INSERT INTO project (code, name, short_description, external_url, logo_file_name) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (code) DO UPDATE
                SET name = excluded.name,
                    short_description = excluded.short_description,
                    external_url = excluded.external_url,
                    logo_file_name = excluded.logo_file_name;""", 
            (code, name, shortDescription, externalUrl, logoFileName)
        )

    def getProjects(self):
        self.cursor.execute("""
            SELECT code, name, logo_file_name
            FROM project;""")
            # ORDER BY count of public datasets
        res = []
        for row in self.cursor:
            res.append(dict(code = row[0], name = row[1], logoFileName = row[2]))
        return res
    
    def existsProject(self, code):
        self.cursor.execute("SELECT code FROM project WHERE code=%s", (code,))
        return self.cursor.rowcount > 0
    
    def getProject(self, code):
        self.cursor.execute("""
            SELECT code, name, short_description, external_url, logo_file_name
            FROM project WHERE code=%s LIMIT 1;""", (code,))
        row = self.cursor.fetchone()
        if row is None: return None
        return dict(code = row[0], name = row[1], shortDescription = row[2],
                    externalUrl = row[3], logoFileName = row[4])
    
    def createOrUpdateSubproject(self, projectCode, code, name, description, externalId):
        self.cursor.execute("""
            INSERT INTO subproject (project_code, code, name, description, external_id) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (project_code, code) DO UPDATE
                SET name = excluded.name,
                    description = excluded.description,
                    external_id = excluded.external_id;""", 
            (projectCode, code, name, description, externalId)
        )
    
    def getSubprojects(self, projectCode):
        self.cursor.execute("""
            SELECT code, name, description, external_id
            FROM subproject WHERE project_code = %s;""", (projectCode,))
        res = []
        for row in self.cursor:
            res.append(dict(code = row[0], name = row[1], description = row[2], externalId = row[3]))
        return res

    def getSubprojectsIDs(self, code):
        self.cursor.execute("SELECT external_id FROM subproject WHERE project_code=%s;", (code,))
        return [row[0] for row in self.cursor]
    
    def setProjectName(self, code, newValue: str | None):
        self.cursor.execute("UPDATE project SET name = %s WHERE code = %s;", (newValue, code))
    def setProjectShortDescription(self, code, newValue: str | None):
        self.cursor.execute("UPDATE project SET short_description = %s WHERE code = %s;", (newValue, code))
    def setProjectExternalUrl(self, code, newValue: str):
        self.cursor.execute("UPDATE project SET external_url = %s WHERE code = %s;", (newValue, code))
    def setProjectLogoFileName(self, code, newValue: str):
        self.cursor.execute("UPDATE project SET logo_file_name = %s WHERE code = %s;", (newValue, code))

    def setProjectConfig(self, projectCode, defaultContactInfo: str, defaultLicenseTitle: str, defaultLicenseUrl: str,
                         zenodoAccessToken: str, zenodoAuthor: str, zenodoCommunity: str, zenodoGrant: str):
        self.cursor.execute("""
            UPDATE project 
                SET default_contact_info = %s, default_license_title = %s, default_license_url = %s,
                    zenodo_access_token = %s, zenodo_author = %s, zenodo_community = %s, zenodo_grant = %s 
            WHERE code = %s;""", 
            (defaultContactInfo, defaultLicenseTitle, defaultLicenseUrl, 
             zenodoAccessToken, zenodoAuthor, zenodoCommunity, zenodoGrant,
             projectCode))

    def getProjectConfig(self, projectCode):
        self.cursor.execute("""
            SELECT default_contact_info, default_license_title, default_license_url,
                   zenodo_access_token, zenodo_author, zenodo_community, zenodo_grant
            FROM project WHERE code=%s LIMIT 1;""", (projectCode,))
        row = self.cursor.fetchone()
        if row is None: return None
        return dict(defaultContactInfo = row[0], 
                    defaultLicense = dict(
                        title = row[1], 
                        url = row[2]),
                    zenodoAccessToken = row[3], zenodoAuthor = row[4], 
                    zenodoCommunity = row[5], zenodoGrant = row[6])
    
