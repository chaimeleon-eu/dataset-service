from psycopg2 import sql
from datetime import datetime
import json
import logging
from .DB import DB
from .. import authorization, output_formats

class DBDatasetsOperator():
    def __init__(self, db: DB):
        self.cursor = db.cursor
        self.conn = db.conn

    def createOrUpdateAuthor(self, userId, username, name, email):
        self.cursor.execute("SELECT id FROM author WHERE id=%s LIMIT 1;", (userId,))
        row = self.cursor.fetchone()
        if row is None: 
            self.cursor.execute("""
                INSERT INTO author (id, username, name, email) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;""", 
                (userId, username, name, email))
        else: 
            self.cursor.execute("""
                UPDATE author
                SET username = %s, name = %s, email = %s
                WHERE id = %s;""", 
                (username, name, email, userId))
    
    def createOrUpdateUser(self, userId, username, gid: int | None = None):
        self.cursor.execute("SELECT id FROM author WHERE id=%s LIMIT 1;", (userId,))
        row = self.cursor.fetchone()
        if row is None: 
            if gid is None: gidstr0 = sql.SQL(""); gidstr1 = sql.SQL("")
            else: gidstr0 = sql.SQL(", gid"); gidstr1 = sql.SQL(", ")+sql.Literal(gid)
            self.cursor.execute(sql.SQL("""
                INSERT INTO author (id, username{}) 
                VALUES ({}, {}{})
                ON CONFLICT (id) DO NOTHING;"""
            ).format(gidstr0, 
                     sql.Literal(str(userId)), sql.Literal(str(username)), gidstr1))
        else: 
            if gid is None: gidstr = sql.SQL("")
            else: gidstr = sql.SQL(", gid=")+sql.Literal(gid)
            self.cursor.execute(sql.SQL("""
                UPDATE author SET username = {}{}
                WHERE id = {};"""
            ).format(sql.Literal(str(username)), gidstr, 
                     sql.Literal(str(userId))))

    def existsUserID(self, id):
        self.cursor.execute("SELECT id FROM author WHERE id=%s", (id,))
        return self.cursor.rowcount > 0

    def getUserIDs(self, userName):
        self.cursor.execute("SELECT id, gid FROM author WHERE username=%s LIMIT 1;", (userName,))
        row = self.cursor.fetchone()
        if row is None: return None, None
        return row[0], row[1]

    def createDataset(self, dataset, userId):
        self.cursor.execute("""
            INSERT INTO dataset (id, name, version, project_code, previous_id, author_id, 
                                 creation_date, description, provenance, purpose, 
                                 type, collection_method, public,
                                 studies_count, subjects_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
            (dataset["id"], dataset["name"], dataset["version"], dataset["project"], dataset["previousId"], userId, 
             dataset["creationDate"], dataset["description"], dataset["provenance"], dataset["purpose"],
             dataset["type"], dataset["collectionMethod"], dataset["public"], 
             dataset["studiesCount"], dataset["subjectsCount"]))

    def updateDatasetAndStudyMetadata(self, dataset):
        # For now let's store directly the final format for each value in the properties of type array of strings,
        # because json don't allow store the None value in a array of strings.
        # In final format the None value is converted to "Unknown" which is compliant with Miabis.
        sexList = [output_formats.sexToMiabis(i) for i in dataset["sex"]]
        diagnosisList = [output_formats.diagnosisToOutputFormat(i) for i in dataset["diagnosis"]]
        bodyPartList = [output_formats.bodyPartToOutputFormat(i) for i in dataset["bodyPart"]]
        modalityList = [output_formats.modalityToOutputFormat(i) for i in dataset["modality"]]
        manufacturerList = [output_formats.manufacturerToOutputFormat(i) for i in dataset["manufacturer"]]
        self.cursor.execute("""
            UPDATE dataset 
            SET studies_count = %s, subjects_count = %s, 
                age_low_in_days = %s, age_low_unit = %s, 
                age_high_in_days = %s, age_high_unit = %s, 
                age_null_count = %s, 
                sex = %s, sex_count = %s, 
                diagnosis = %s, diagnosis_count = %s,
                diagnosis_year_low = %s, diagnosis_year_high = %s, 
                diagnosis_year_null_count = %s, 
                body_part = %s, body_part_count = %s, 
                modality = %s, modality_count = %s, 
                manufacturer = %s, manufacturer_count = %s, 
                series_tags = %s, size_in_bytes = %s
            WHERE id = %s;""", 
            (dataset["studiesCount"], dataset["subjectsCount"], 
                dataset["ageLowInDays"], dataset["ageLowUnit"], 
                dataset["ageHighInDays"], dataset["ageHighUnit"], 
                dataset["ageNullCount"], 
                json.dumps(sexList), json.dumps(dataset["sexCount"]), 
                json.dumps(diagnosisList), json.dumps(dataset["diagnosisCount"]), 
                dataset["diagnosisYearLow"], dataset["diagnosisYearHigh"], 
                dataset["diagnosisYearNullCount"], 
                json.dumps(bodyPartList), json.dumps(dataset["bodyPartCount"]), 
                json.dumps(modalityList), json.dumps(dataset["modalityCount"]), 
                json.dumps(manufacturerList), json.dumps(dataset["manufacturerCount"]), 
                json.dumps(dataset["seriesTags"]), dataset["sizeInBytes"],
                dataset["id"]))
        for study in dataset["studies"]:
            self.cursor.execute("""
                UPDATE dataset_study set size_in_bytes=%s 
                WHERE dataset_id = %s AND study_id = %s;""",
                (study['sizeInBytes'], dataset["id"], study['studyId']))
            self.cursor.execute("""
                UPDATE study
                SET age_in_days = %s, sex = %s, 
                    diagnosis = %s, diagnosis_year = %s, study_date = %s 
                WHERE id = %s;""", 
                (study['ageInDays'], study['sex'], 
                 study['diagnosis'], study['diagnosisYear'], study['studyDate'],
                 study['studyId']))
            for series in study['series']:
                self.cursor.execute("""
                    UPDATE series
                    SET body_part = %s, modality = %s, manufacturer = %s
                    WHERE study_id = %s AND folder_name = %s;""", 
                    (series['bodyPart'], series['modality'], series['manufacturer'], 
                     study['studyId'], series['folderName']))

    def createDatasetCreationStatus(self, datasetId, status, firstMessage):
        self.cursor.execute("""
            INSERT INTO dataset_creation_status (dataset_id, status, last_message)
            VALUES (%s,%s,%s);""",
            (datasetId, status, firstMessage))
    def setDatasetCreationStatus(self, datasetId, status, lastMessage):
        self.cursor.execute("""
            UPDATE dataset_creation_status 
            SET status = %s, last_message = %s
            WHERE dataset_id = %s;""",
            (status, lastMessage, datasetId))
    def getDatasetCreationStatus(self, datasetId):
        """Returns None if the dataset creation status not exists.
        """
        self.cursor.execute("""
            SELECT dataset_id, status, last_message
            FROM dataset_creation_status 
            WHERE dataset_id=%s 
            LIMIT 1;""",
            (datasetId,))
        row = self.cursor.fetchone()
        if row is None: return None
        return dict(datasetId = row[0], status = row[1], lastMessage = row[2])
    def deleteDatasetCreationStatus(self, datasetId):
        self.cursor.execute("DELETE FROM dataset_creation_status WHERE dataset_id=%s;", (datasetId,))

    def createOrUpdateStudy(self, study, datasetId):
        self.cursor.execute("""
            INSERT INTO study (id, name, subject_name, path_in_datalake, url)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE
                SET name = excluded.name,
                    subject_name = excluded.subject_name,
                    path_in_datalake = excluded.path_in_datalake,
                    url = excluded.url;""",
            (study["studyId"], study["studyName"], study["subjectName"], 
             study["pathInDatalake"], study["url"]))
        self.cursor.execute("""
            INSERT INTO dataset_study (dataset_id, study_id, series)
            VALUES (%s,%s,%s);""",
            (datasetId, study["studyId"], json.dumps(study["series"])))
        self.createSeries(datasetId, study["studyId"], study['series'])

    def createSeries(self, datasetId, studyId, studySeries):
        createdSeries = set()
        for series in studySeries:
            if series["folderName"] in createdSeries:
                logging.root.error("There are two series in the same folder name "
                    +"[datasetId: %s, studyId: %s, folder: %s]" % (datasetId, studyId, series["folderName"]))
                continue
            createdSeries.add(series["folderName"])
            self.cursor.execute("""
                INSERT INTO series (study_id, folder_name)
                VALUES (%s,%s)
                ON CONFLICT (study_id, folder_name) DO NOTHING;""",
                (studyId, series["folderName"]))
            self.cursor.execute("""
                INSERT INTO dataset_study_series (dataset_id, study_id, series_folder_name)
                VALUES (%s,%s,%s);""",
                (datasetId, studyId, series["folderName"]))

    def setDatasetStudyHash(self, datasetId, studyId, hash):
        self.cursor.execute("""
            UPDATE dataset_study set hash=%s 
            WHERE dataset_id = %s AND study_id = %s;""",
            (hash, datasetId, studyId))

    # def getDatasetStudyHash(self, datasetId, studyId):
    #     self.cursor.execute("""
    #         SELECT hash FROM dataset_study 
    #         WHERE dataset_id = %s AND study_id = %s;""",
    #         (datasetId, studyId))
    #     if self.cursor.rowcount == 0: return None
    #     row = self.cursor.fetchone()
    #     return row[0]

    def setSeriesHashCache(self, studyId, seriesDirName, hash, time: datetime):
        self.cursor.execute("""
            UPDATE series set hash_cache=%s, hash_last_time_calculated=%s
            WHERE study_id = %s AND folder_name = %s;""",
            (hash, time, studyId, seriesDirName))

    def getSeriesHashCache(self, studyId, seriesDirName) -> tuple[bytes|None, datetime|None]:
        self.cursor.execute("""
            SELECT hash_cache, hash_last_time_calculated FROM series
            WHERE study_id = %s AND folder_name = %s;""",
            (studyId, seriesDirName))
        row = self.cursor.fetchone()
        if row is None: return None, None
        return row[0], row[1]

    def existsDataset(self, id):
        """Note: invalidated datasets also exist.
        """
        self.cursor.execute("SELECT id FROM dataset WHERE id=%s", (id,))
        return self.cursor.rowcount > 0

    PREFERRED_ZENODO = "zenodo"

    def getDataset(self, id):
        """Returns None if the dataset not exists.
        """
        self.cursor.execute("""
            SELECT dataset.id, dataset.name, dataset.previous_id, 
                   author.id, author.name, author.email, 
                   dataset.creation_date, dataset.description, 
                   dataset.license_title, dataset.license_url, 
                   dataset.pid_url, dataset.zenodo_doi, dataset.contact_info, 
                   dataset.draft, dataset.public, dataset.invalidated, 
                   dataset.studies_count, dataset.subjects_count, 
                   dataset.age_low_in_days, dataset.age_low_unit, 
                   dataset.age_high_in_days, dataset.age_high_unit, 
                   dataset.age_null_count, 
                   dataset.sex, dataset.sex_count, 
                   dataset.diagnosis_year_low, dataset.diagnosis_year_high, 
                   dataset.diagnosis_year_null_count, 
                   dataset.body_part, dataset.body_part_count, 
                   dataset.modality, dataset.modality_count, 
                   dataset.manufacturer, dataset.manufacturer_count, 
                   dataset.series_tags, 
                   dataset.next_id, dataset.last_integrity_check, dataset.size_in_bytes, 
                   dataset.project_code, dataset.version, 
                   dataset.purpose, dataset.type, dataset.collection_method,
                   dataset.invalidation_reason, dataset.corrupted, dataset.provenance,
                   dataset.diagnosis, dataset.diagnosis_count,
                   dataset.tags
            FROM dataset, author 
            WHERE dataset.id=%s AND author.id = dataset.author_id 
            LIMIT 1;""",
            (id,))
        row = self.cursor.fetchone()
        if row is None: return None
        creationDate = str(row[6].astimezone())   # row[6] is a datetime without time zone, just add the local tz.
                                                  # If local tz is UTC, the string "+00:00" is added at the end.
        lastIntegrityCheck = None if row[36] is None else str(row[36].astimezone())
        if row[18] is None:
            ageLow, ageHigh = None, None
            ageUnit = []
        else:
            ageLow, ageLowUnit = output_formats.ageToMiabis(row[18], row[19])
            ageHigh, ageHighUnit = output_formats.ageToMiabis(row[20], row[21])
            ageUnit = [ageLowUnit, ageHighUnit]
        if row[10] is None:
            prefPid = None
            customPidUrl = None
        elif row[10] == self.PREFERRED_ZENODO:
            prefPid = "zenodoDoi"
            customPidUrl = None
        else: 
            prefPid = "custom"
            customPidUrl = row[10]
        
        ds = dict(id = row[0], name = row[1], version = row[39], project = row[38],
                    previousId = row[2], nextId = row[35], 
                    authorId = row[3], authorName = row[4], authorEmail = row[5], 
                    creationDate = creationDate, description = row[7], tags = row[48], 
                    provenance = row[45], purpose = row[40], type = row[41], collectionMethod = row[42],
                    license = dict(
                        title = row[8], 
                        url = row[9]), 
                    contactInfo = row[12],
                    pids = dict(
                        preferred = prefPid, 
                        urls = dict(
                            zenodoDoi = row[11], 
                            custom = customPidUrl)), 
                    draft = row[13], public = row[14], invalidated = row[15], 
                    corrupted = row[44], lastIntegrityCheck = lastIntegrityCheck, 
                    studiesCount = row[16], subjectsCount = row[17], 
                    ageLow = ageLow, ageHigh = ageHigh, ageUnit = ageUnit, ageNullCount = row[22], 
                    sex = json.loads(row[23]), sexCount = json.loads(row[24]), 
                    diagnosis = json.loads(row[46]), diagnosisCount = json.loads(row[47]),
                    diagnosisYearLow = row[25], diagnosisYearHigh = row[26], diagnosisYearNullCount = row[27], 
                    bodyPart = json.loads(row[28]), bodyPartCount = json.loads(row[29]), 
                    modality = json.loads(row[30]), modalityCount = json.loads(row[31]), 
                    manufacturer = json.loads(row[32]), manufacturerCount = json.loads(row[33]), 
                    seriesTags = json.loads(row[34]), 
                    sizeInBytes = row[37])
        if ds["invalidated"]: ds["invalidationReason"] = row[43]
        return ds

    def getStudiesFromDataset(self, datasetId, limit = 0, skip = 0):
        if limit == 0: limit = 'ALL'

        # First get total rows without LIMIT and OFFSET
        self.cursor.execute("SELECT count(*) FROM dataset_study WHERE dataset_study.dataset_id = %s", (datasetId,))
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0

        self.cursor.execute(sql.SQL("""
            SELECT study.id, study.name, study.subject_name, study.url, study.path_in_datalake, 
                   dataset_study.series, dataset_study.hash, dataset_study.size_in_bytes
            FROM study, dataset_study 
            WHERE dataset_study.dataset_id = %s AND dataset_study.study_id = study.id 
            ORDER BY study.name 
            LIMIT {} OFFSET {};""").format(sql.SQL(str(limit)), sql.SQL(str(skip))),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(dict(studyId = row[0], studyName = row[1], subjectName = row[2], pathInDatalake = row[4],
                            series = json.loads(row[5]), url = row[3], hash = row[6], sizeInBytes = row[7]))
        return res, total

    def getPathsOfStudiesFromDataset(self, datasetId):
        self.cursor.execute(sql.SQL("""
            SELECT study.id, study.path_in_datalake 
            FROM study, dataset_study 
            WHERE dataset_study.dataset_id = %s AND dataset_study.study_id = study.id;"""),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(row[1])
        return res

    def getDatasets(self, skip, limit, searchString, searchFilter: authorization.Search_filter, 
                    sortBy = 'creationDate', sortDirection = '', searchSubject: str = '', 
                    onlyLastVersions: bool = False):
        fromExtra = sql.Composed([])
        whereClause = sql.Composed([])

        if searchFilter.invalidated == False:
            whereClause += sql.SQL(" AND dataset.invalidated = false")
        elif searchFilter.invalidated == True:
            whereClause += sql.SQL(" AND dataset.invalidated = true")
        else: # searchFilter.invalidated is None:
            if searchFilter.getUserId() != None:
                authorId = sql.Literal(str(searchFilter.getUserId()))
                whereClause += sql.SQL(" AND ({} OR {})").format(
                    sql.SQL("(dataset.invalidated = true AND dataset.author_id = {})").format(authorId),
                    sql.SQL("dataset.invalidated = false")
                )

        if searchFilter.draft == False:
            whereClause += sql.SQL(" AND dataset.draft = false")
        elif searchFilter.draft == True:
            whereClause += sql.SQL(" AND dataset.draft = true")
        else: # searchFilter.draft is None:
            if searchFilter.getUserId() != None:
                authorId = sql.Literal(str(searchFilter.getUserId()))
                whereClause += sql.SQL(" AND ({} OR {})").format(
                    sql.SQL("(dataset.draft = true AND dataset.author_id = {})").format(authorId),
                    sql.SQL("dataset.draft = false")
                )

        if (searchFilter.draft == True or searchFilter.invalidated == True) \
            and searchFilter.getUserId() != None:
            authorId = sql.Literal(str(searchFilter.getUserId()))
            whereClause += sql.SQL(" AND dataset.author_id = ") + authorId
        
        nonPublicCondition = sql.SQL("")
        projectsForNonPublic = searchFilter.getProjectsForNonPublic()
        if projectsForNonPublic != None:
            if len(projectsForNonPublic) > 0:
                projectsForNonPublic = sql.SQL(', ').join(sql.Literal(item) for item in projectsForNonPublic)
            else: projectsForNonPublic = sql.Literal('--no---project--')
            nonPublicCondition = sql.SQL(" AND dataset.project_code IN ({})").format(projectsForNonPublic)

        publicCondition = sql.SQL("")
        projectsForPublic = searchFilter.getProjectsForPublic()
        if projectsForPublic != None:
            if len(projectsForPublic) > 0:
                projectsForPublic = sql.SQL(', ').join(sql.Literal(item) for item in projectsForPublic)
            else: projectsForPublic = sql.Literal('--no---project--')
            publicCondition = sql.SQL(" AND dataset.project_code IN ({})").format(projectsForPublic)

        if searchFilter.public == False:
            whereClause += sql.SQL(" AND dataset.public = false {}").format(nonPublicCondition)
        elif searchFilter.public == True:
            whereClause += sql.SQL(" AND dataset.public = true {}").format(publicCondition)
        else: # searchFilter.public is None:
            if projectsForNonPublic != None or projectsForPublic != None:
                whereClause += sql.SQL(" AND ({} OR {})").format(
                    sql.SQL("(dataset.public = false {})").format(nonPublicCondition),
                    sql.SQL("(dataset.public = true {})").format(publicCondition)
                )
        
        if searchString != '': 
            s = sql.Literal('%'+searchString+'%')
            whereClause += sql.SQL(
                    " AND ( dataset.name ILIKE {} OR dataset.id LIKE {} OR author.name ILIKE {})"
                ).format(s, s, s)
        
        if searchSubject != '':
            fromExtra += sql.SQL(", dataset_study, study")
            s = sql.Literal('%'+searchSubject+'%')
            whereClause += sql.SQL(
                    " AND dataset.id = dataset_study.dataset_id AND dataset_study.study_id = study.id"
                    + " AND study.subject_name ILIKE {}"
                ).format(s)
        
        if onlyLastVersions:
            whereClause += sql.SQL(" AND dataset.next_id IS NULL")
        
        if len(searchFilter.tags) > 0:
            whereClause += sql.SQL(
                    " AND dataset.tags @> ARRAY[{}]::VARCHAR[]"
                ).format(sql.SQL(', ').join(sql.Literal(item) for item in searchFilter.tags))
        
        default = 'dataset.creation_date DESC'
        if sortBy == 'name':
            dir = 'DESC' if sortDirection == 'descending' else 'ASC'
            sortByClause = 'dataset.name %s, %s' % (dir, default)
        elif sortBy == 'authorName':
            dir = 'DESC' if sortDirection == 'descending' else 'ASC'
            sortByClause = 'author.name %s, %s' % (dir, default)        
        elif sortBy == 'studiesCount':
            dir = 'ASC' if sortDirection == 'ascending' else 'DESC'
            sortByClause = 'dataset.studies_count %s, %s' % (dir, default)
        elif sortBy == 'subjectsCount':
            dir = 'ASC' if sortDirection == 'ascending' else 'DESC'
            sortByClause = 'dataset.subjects_count %s, %s' % (dir, default)
        else:  # sortBy == 'creationDate' or ''
            dir = 'ASC' if sortDirection == 'ascending' else 'DESC'
            sortByClause = 'dataset.creation_date %s' % dir

        if limit == 0: limit = 'ALL'

        # First get total rows without LIMIT and OFFSET
        self.cursor.execute(sql.SQL("""
            SELECT count(*) FROM dataset, author{}
            WHERE dataset.author_id = author.id {}""").format(fromExtra, whereClause))
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0

        q = sql.SQL("""
                SELECT dataset.id, dataset.name, author.name, dataset.creation_date, dataset.project_code, 
                    dataset.draft, dataset.public, dataset.invalidated, dataset.corrupted,
                    dataset.studies_count, dataset.subjects_count, dataset.version, dataset.tags
                FROM dataset, author{}
                WHERE dataset.author_id = author.id {}
                ORDER BY {} 
                LIMIT {} OFFSET {};"""
            ).format(fromExtra, whereClause, sql.SQL(str(sortByClause)), sql.SQL(str(limit)), sql.SQL(str(skip)))
        logging.root.debug("QUERY: " + q.as_string(self.conn))
        self.cursor.execute(q)
        res = []
        for row in self.cursor:
            creationDate = str(row[3].astimezone())   # row[3] is a datetime without time zone, just add the local tz.
                                                      # If local tz is UTC, the string "+00:00" is added at the end.
            res.append(dict(id = row[0], name = row[1], version = row[11], authorName = row[2], creationDate = creationDate, project = row[4],
                            draft = row[5], public = row[6], invalidated = row[7], corrupted = row[8], tags = row[12],
                            studiesCount = row[9], subjectsCount = row[10]))
        return res, total
    
    def getProjectsForSearchFilter(self, searchFilter: authorization.Search_filter):
        whereClause = sql.Composed([])

        if searchFilter.getUserId() != None:
            authorId = sql.Literal(str(searchFilter.getUserId()))
            whereClause += sql.SQL(" AND ({} OR {})").format(
                sql.SQL("(dataset.invalidated = true AND dataset.author_id = {})").format(authorId),
                sql.SQL("dataset.invalidated = false")
            )
            whereClause += sql.SQL(" AND ({} OR {})").format(
                sql.SQL("(dataset.draft = true AND dataset.author_id = {})").format(authorId),
                sql.SQL("dataset.draft = false")
            )

        projectsForNonPublic = searchFilter.getProjectsForNonPublic()
        if projectsForNonPublic != None:
            if len(projectsForNonPublic) > 0:
                projectsForNonPublic = sql.SQL(', ').join(sql.Literal(item) for item in projectsForNonPublic)
            else: projectsForNonPublic = sql.Literal('--no---project--')
            nonPublicCondition = sql.SQL(" AND dataset.project_code IN ({})").format(projectsForNonPublic)
            whereClause += sql.SQL(" AND ({} OR {})").format(
                sql.SQL("(dataset.public = false {})").format(nonPublicCondition),
                sql.SQL("dataset.public = true")
            )

        self.cursor.execute(sql.SQL("""
                SELECT DISTINCT dataset.project_code
                FROM dataset, author
                WHERE dataset.author_id = author.id {};"""
            ).format(whereClause))    
        res = []
        for row in self.cursor:
            res.append(row[0])
        return res
    
    def getUpgradableDatasets(self, filter: authorization.Upgradables_filter):
        whereClause = sql.SQL("")
        if filter.getUserId() != None:
            authorId = sql.Literal(str(filter.getUserId()))
            whereClause = sql.SQL(" AND author_id = ") + authorId
        self.cursor.execute(sql.SQL("""
            SELECT id, name, version
            FROM dataset
            WHERE draft = false AND next_id is NULL {}
            ORDER BY name;""").format(whereClause))
        res = []
        for row in self.cursor:
            res.append(dict(id = row[0], name = row[1], version = row[2]))
        return res
    
    def getDatasetsSharingPreviousId(self, previousId):
        self.cursor.execute(sql.SQL("""
            SELECT id
            FROM dataset
            WHERE previous_id = {};"""
            ).format(sql.Literal(previousId)))
        res = []
        for row in self.cursor:
            res.append(row[0])
        return res
    
    def getDatasetACL(self, datasetId):
        self.cursor.execute(sql.SQL(
            "SELECT user_id FROM dataset_acl WHERE dataset_id = %s;"), (datasetId,))
        return [row[0] for row in self.cursor]

    def getDatasetACL_detailed(self, datasetId):
        self.cursor.execute(sql.SQL("""
            SELECT author.id, author.username
            FROM dataset_acl, author
            WHERE dataset_acl.dataset_id = %s AND dataset_acl.user_id = author.id
            ORDER BY author.username;"""),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(dict(uid = row[0], username = row[1]))
        return res
    
    def addUserToDatasetACL(self, datasetId, newUserId):
        self.cursor.execute("""
                INSERT INTO dataset_acl (dataset_id, user_id) 
                VALUES (%s, %s)
                ON CONFLICT (dataset_id, user_id) DO NOTHING;""", 
                (datasetId, newUserId))
        
    def deleteUserFromDatasetACL(self, datasetId, userId):
        self.cursor.execute(
            "DELETE FROM dataset_acl WHERE dataset_id=%s AND user_id = %s;", 
            (datasetId, userId))

    def clearDatasetACL(self, datasetId):
        self.cursor.execute(
            "DELETE FROM dataset_acl WHERE dataset_id=%s;", (datasetId,))
    
    def deleteDataset(self, datasetId):
        self.cursor.execute("DELETE FROM dataset_creation_status WHERE dataset_id=%s;", (datasetId,))
        self.cursor.execute("DELETE FROM dataset_study WHERE dataset_id=%s;", (datasetId,))
        self.cursor.execute("DELETE FROM dataset_study_series WHERE dataset_id=%s;", (datasetId,))
        self.cursor.execute("DELETE FROM dataset WHERE id=%s;", (datasetId,))

    def deleteOrphanStudies(self):
        '''This is a kind of garbage-collection that deletes all the studies not included in any dataset_study.'''
        self.cursor.execute("""
            SELECT COUNT(*) FROM study as s
            WHERE not exists (select ds.study_id 
                              from dataset_study as ds
                              where ds.study_id = s.id);""")
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0
        if total == 0:
            logging.root.debug("There are no orphan studies to remove (all of them were included in datasets).")
            return
        logging.root.debug("There are %d orphan studies (not included in any dataset), removing..." % total )
        self.cursor.execute("""
            DELETE FROM study as s
            WHERE not exists (select ds.study_id 
                              from dataset_study as ds
                              where ds.study_id = s.id);""")

    def deleteOrphanSeries(self):
        '''This is a kind of garbage-collection that deletes all the series not included in any dataset_study_series.'''
        self.cursor.execute("""
            SELECT COUNT(*) FROM series as s
            WHERE not exists (select dss.study_id, dss.series_folder_name 
                              from dataset_study_series as dss
                              where dss.study_id = s.study_id and dss.series_folder_name = s.folder_name );""")
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0
        if total == 0:
            logging.root.debug("There are no orphan series to remove (all of them were included in datasets).")
            return
        logging.root.debug("There are %d orphan series (not included in any dataset), removing..." % total )
        self.cursor.execute("""
            DELETE FROM series as s
            WHERE not exists (select dss.study_id, dss.series_folder_name 
                              from dataset_study_series as dss
                              where dss.study_id = s.study_id and dss.series_folder_name = s.folder_name );""")

    def getLicenses(self):
        self.cursor.execute("""
            SELECT name, url
            FROM license
            ORDER BY name;""")
        res = []
        for row in self.cursor:
            res.append(dict(title = row[0], url = row[1]))
        return res
    
    def getLicense(self, id):
        self.cursor.execute("""
            SELECT name, url FROM license WHERE id=%s LIMIT 1;""", (id,))
        row = self.cursor.fetchone()
        if row is None: return None
        return dict(title = row[0], url = row[1])

    def setZenodoDOI(self, id, newValue: str | None):
        self.cursor.execute("UPDATE dataset SET zenodo_doi = %s WHERE id = %s;", (newValue, id))

    def setDatasetInvalidated(self, id, newValue: bool):
        self.cursor.execute("UPDATE dataset SET invalidated = %s WHERE id = %s;", (newValue, id))

    def setDatasetInvalidationReason(self, id, newValue: str | None):
        self.cursor.execute("UPDATE dataset SET invalidation_reason = %s WHERE id = %s;", (newValue, id))

    def setDatasetPublic(self, id, newValue: bool):
        self.cursor.execute("UPDATE dataset SET public = %s WHERE id = %s;", (newValue, id))
        
    def setDatasetDraft(self, id, newValue: bool):
        self.cursor.execute("UPDATE dataset SET draft = %s WHERE id = %s;", (newValue, id))

    def setDatasetName(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET name = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetVersion(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET version = %s WHERE id = %s;", (newValue, id))

    def setDatasetDescription(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET description = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetTags(self, id, newValue: list[str]):
        self.cursor.execute("UPDATE dataset SET tags = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetProvenance(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET provenance = %s WHERE id = %s;", (newValue, id))

    def setDatasetPurpose(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET purpose = %s WHERE id = %s;", (newValue, id))

    def setDatasetPreviousId(self, id, newValue: str | None):
        self.cursor.execute("UPDATE dataset SET previous_id = %s WHERE id = %s;", (newValue, id))

    def setDatasetNextId(self, id, newValue: str | None):
        self.cursor.execute("UPDATE dataset SET next_id = %s WHERE id = %s;", (newValue, id))

    def setDatasetType(self, id, newValue: list[str]):
        self.cursor.execute("UPDATE dataset SET type = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetCollectionMethod(self, id, newValue: list[str]):
        self.cursor.execute("UPDATE dataset SET collection_method = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetLicense(self, datasetId, newTitle: str, newUrl: str):
        self.cursor.execute("UPDATE dataset SET license_title = %s, license_url = %s WHERE id = %s;", 
                            (newTitle, newUrl, datasetId))

    def setDatasetPid(self, id, preferred: str, custom: str | None = None):
        newValue = self.PREFERRED_ZENODO if preferred == "zenodoDoi" else custom
        self.cursor.execute("UPDATE dataset SET pid_url = %s WHERE id = %s;", (newValue, id))

    def setDatasetContactInfo(self, id, newValue: str | None):
        self.cursor.execute("UPDATE dataset SET contact_info = %s WHERE id = %s;", (newValue, id))
    
    def setDatasetAuthor(self, id, newValue: str):
        self.cursor.execute("UPDATE dataset SET author_id = %s WHERE id = %s;", (newValue, id))

    def setDatasetLastIntegrityCheck(self, id, newStatusCorrupted: bool, newDate: datetime | None):
        self.cursor.execute("UPDATE dataset SET corrupted = %s, last_integrity_check = %s WHERE id = %s;", 
                            (newStatusCorrupted, newDate, id))
