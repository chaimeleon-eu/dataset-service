from array import array
import logging
import psycopg2
from psycopg2 import sql
import json
from dataset_service import dicom

class DB:
    def __init__(self, dbConfig):
        self.conn = psycopg2.connect(host=dbConfig.host, port=dbConfig.port, dbname=dbConfig.dbname, user=dbConfig.user, password=dbConfig.password)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if tb is None:
            # No exception, so commit
            self.conn.commit()
        else:
            # Exception occurred, so rollback.
            self.conn.rollback()
        self.cursor.close()
        self.conn.close()
        return False   # if an exception has been raised then it will be re-raised
        
    def close(self):
        self.cursor.close()
        self.conn.close()

    def setup(self):
        version = self.getSchemaVersion()
        logging.root.info("Database schema version: %d " % version)
        if version == 0:
            logging.root.info("Creating tables...")
            self.createSchema()
            return
        else:
            if version <=1: self.updateDB_v1To2()
            if version <=2: self.updateDB_v2To3()
            if version <=3: self.updateDB_v3To4()
            if version <=4: self.updateDB_v4To5()
            if version <=5: self.updateDB_v5To6()
            ### Finally update schema_version
            self.cursor.execute("UPDATE metadata set schema_version = 6;")

    def getSchemaVersion(self):
        self.cursor.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = 'metadata');")
        if self.cursor.fetchone()[0] == False: 
            return 0
        self.cursor.execute("SELECT schema_version FROM metadata limit 1")
        return self.cursor.fetchone()[0]

    def createSchema(self):
        #with open("schema.sql", 'r') as inputStream:
        #    self.cursor.execute(inputStream.read())

        self.cursor.execute("""
            CREATE TABLE metadata (
                id integer DEFAULT 1 NOT NULL CHECK (id = 1),
                schema_version integer NOT NULL,
                constraint pk_metadata primary key (id)
            );
            INSERT INTO metadata (schema_version) 
            VALUES ('5')
            ON CONFLICT (id) DO UPDATE
                SET schema_version = excluded.schema_version;
                
            CREATE SEQUENCE gid_sequence increment 1 start 2000;
            CREATE TABLE author (
                id varchar(64),
                username varchar(64) DEFAULT NULL,
                gid integer NOT NULL DEFAULT nextval('gid_sequence'),
                name varchar(128),
                email varchar(128),
                constraint pk_user primary key (id),
                constraint un_user unique (username),
                constraint un_gid unique (gid)
            );
            CREATE TABLE user_group (
                user_id varchar(64),
                group_name varchar(128),
                constraint pk_user_group primary key (user_id, group_name),
                constraint fk_user foreign key (user_id) references author(id)
            );
            CREATE TABLE study (
                id varchar(32),
                name varchar(128) NOT NULL,
                subject_name varchar(128) NOT NULL,
                path varchar(256),
                url varchar(256),
                constraint pk_study primary key (id)
            );
            CREATE TABLE dataset (
                id varchar(40),
                name varchar(256) NOT NULL,
                previous_id varchar(32) DEFAULT NULL,
                author_id varchar(64) NOT NULL,
                creation_date timestamp NOT NULL,
                description text NOT NULL DEFAULT '',
                public boolean NOT NULL DEFAULT false,
                invalidated boolean NOT NULL DEFAULT false,
                studies_count integer NOT NULL,
                subjects_count integer NOT NULL,
                age_low varchar(4) DEFAULT NULL,
                age_high varchar(4) DEFAULT NULL,
                sex varchar(8) NOT NULL DEFAULT '[]',
                body_part text NOT NULL DEFAULT '[]',
                modality text NOT NULL DEFAULT '[]',
                constraint pk_dataset primary key (id),
                constraint fk_author foreign key (author_id) references author(id)
            );
            /* A dataset can contain multiple studies and a study can be contained in multiple datasets. */
            CREATE TABLE dataset_study (
                dataset_id varchar(40),
                study_id varchar(32),
                series text NOT NULL DEFAULT '[]',
                constraint pk_dataset_study primary key (dataset_id, study_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
                constraint fk_study foreign key (study_id) references study(id)
            );
            CREATE TABLE dataset_access (
                id varchar(40),
                user_gid integer,
                tool_name varchar(256),
                tool_version varchar(256),
                constraint pk_dataset_access primary key (id),
                constraint fk_user foreign key (user_gid) references author(gid)
            );
            CREATE TABLE dataset_access_dataset (
                dataset_access_id varchar(128),
                dataset_id varchar(40),
                constraint pk_dataset_access_dataset primary key (dataset_access_id, dataset_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id)
            );
        """)
    
    def updateDB_v1To2(self):
        logging.root.info("Updating database from v1 to v2...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN invalidated boolean NOT NULL DEFAULT false;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN studies_count integer NOT NULL DEFAULT 0;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN patients_count integer NOT NULL DEFAULT 0;")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN studies_count DROP DEFAULT;")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN patients_count DROP DEFAULT;")

    def updateDB_v2To3(self):
        logging.root.info("Updating database from v2 to v3...")
        self.cursor.execute("ALTER TABLE author ADD COLUMN username varchar(64) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE author ADD constraint un_user unique (username);")
        self.cursor.execute("CREATE SEQUENCE gid_sequence increment 1 start 2000;")
        self.cursor.execute("ALTER TABLE author ADD COLUMN gid integer;")
        self.cursor.execute("SELECT id FROM author;")
        ids = []
        for row in self.cursor:
            ids.append(row[0])
        for id in ids:
            self.cursor.execute("UPDATE author SET gid=nextval('gid_sequence') WHERE id=%s;", (id,))
        self.cursor.execute("ALTER TABLE author ALTER COLUMN gid SET NOT NULL;")
        self.cursor.execute("ALTER TABLE author ADD constraint un_gid unique (gid);")
        self.cursor.execute("ALTER TABLE author ALTER COLUMN gid SET DEFAULT nextval('gid_sequence');")
        self.cursor.execute("""CREATE TABLE user_group (
                                    user_id varchar(64),
                                    group_name varchar(128),
                                    constraint pk_user_group primary key (user_id, group_name)
                                );""")

    def updateDB_v3To4(self):
        logging.root.info("Updating database from v3 to v4...")
        self.cursor.execute("""CREATE TABLE dataset_access (
                                    id varchar(40),
                                    user_gid integer,
                                    tool_name varchar(256),
                                    tool_version varchar(256),
                                    constraint pk_dataset_access primary key (id),
                                    constraint fk_user foreign key (user_gid) references author(gid)
                                );""")
        self.cursor.execute("""CREATE TABLE dataset_access_dataset (
                                    dataset_access_id varchar(128),
                                    dataset_id varchar(40),
                                    constraint pk_dataset_access_dataset primary key (dataset_access_id, dataset_id),
                                    constraint fk_dataset foreign key (dataset_id) references dataset(id)
                                );""")
        self.cursor.execute("ALTER TABLE dataset DROP COLUMN gid;")
        self.cursor.execute("ALTER TABLE user_group ADD constraint fk_user foreign key (user_id) references author(id);")
                    
    def updateDB_v4To5(self):
        logging.root.info("Updating database from v4 to v5...")
        self.cursor.execute("ALTER TABLE dataset_study ADD COLUMN series text NOT NULL DEFAULT '[]';")
        
    def updateDB_v5To6(self):
        logging.root.info("Updating database from v5 to v6...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_low varchar(4) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_high varchar(4) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN sex varchar(8) NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN body_part text NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN modality text NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset RENAME COLUMN patients_count TO subjects_count;")


    def createOrUpdateAuthor(self, userId, username, name, email):
        self.cursor.execute("""
            INSERT INTO author (id, username, name, email) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
                SET username = excluded.username,
                    name = excluded.name,
                    email = excluded.email;""", 
            (userId, username, name, email)
        )
    
    def createOrUpdateUser(self, userId, userName, groups):
        self.cursor.execute("""
            INSERT INTO author (id, username) 
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE
                SET username = excluded.username;""", 
            (userId, userName)
        )
        # delete and reintroduce the groups because they can have been changed
        self.cursor.execute("DELETE FROM user_group WHERE user_id=%s;", (userId,))
        for group in groups:
            self.cursor.execute("""
                INSERT INTO user_group (user_id, group_name) 
                VALUES (%s, %s);""", 
                (userId, group)
            )

    def getUserIDs(self, userName):
        self.cursor.execute("SELECT id, gid FROM author WHERE username=%s LIMIT 1;", (userName,))
        row = self.cursor.fetchone()
        if row is None: return None
        return row[0], row[1]

    def getUserGroups(self, userName):
        self.cursor.execute("""
            SELECT user_group.group_name 
            FROM author, user_group 
            WHERE author.username=%s AND author.id = user_group.user_id;""", 
            (userName,))
        res = []
        for row in self.cursor:
            res.append(row[0])
        return res

    def createDataset(self, dataset, userId):
        self.cursor.execute("""
            INSERT INTO dataset (id, name, previous_id, author_id, 
                                 creation_date, description, public,
                                 studies_count, subjects_count, 
                                 age_low, age_high, 
                                 sex, body_part, modality)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
            (dataset["id"], dataset["name"], dataset["previousId"], userId, 
             dataset["creationDate"], dataset["description"], dataset["public"], 
             dataset["studiesCount"], dataset["subjectsCount"], 
             dataset["ageLow"], dataset["ageHigh"], 
             json.dumps(dataset["sex"]), json.dumps(dataset["bodyPart"]), 
             json.dumps(dataset["modality"])))

    def createOrUpdateStudy(self, study, datasetId):
        self.cursor.execute("""
            INSERT INTO study (id, name, subject_name, path, url)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE
                SET name = excluded.name,
                    subject_name = excluded.subject_name,
                    path = excluded.path,
                    url = excluded.url;""",
            (study["studyId"], study["studyName"], study["subjectName"], 
             study["path"], study["url"]))
        self.cursor.execute("""
            INSERT INTO dataset_study (dataset_id, study_id, series)
            VALUES (%s,%s,%s);""",
            (datasetId, study["studyId"], json.dumps(study["series"])))


    def existDataset(self, id):
        """Note: invalidated datasets also exist.
        """
        self.cursor.execute("SELECT id FROM dataset WHERE id=%s", (id,))
        return self.cursor.rowcount > 0

    def getDataset(self, id):
        """Returns None if the dataset has been invalidated or not exists.
        """
        self.cursor.execute("""
            SELECT dataset.id, dataset.name, dataset.previous_id, 
                   author.id, author.name, author.email, 
                   dataset.creation_date, dataset.description, dataset.public, 
                   dataset.studies_count, dataset.subjects_count, 
                   dataset.age_low, dataset.age_high, 
                   dataset.sex, dataset.body_part, dataset.modality 
            FROM dataset, author 
            WHERE dataset.id=%s AND author.id = dataset.author_id AND dataset.invalidated = false
            LIMIT 1;""",
            (id,))
        row = self.cursor.fetchone()
        if row is None: return None
        if row[11] is None:
            ageLow = None
            ageHigh = None
            ageUnit = None
        else:
            ageLow, ageLowUnit = dicom.getAgeInMiabisFormat(row[11])
            ageHigh, ageHighUnit = dicom.getAgeInMiabisFormat(row[12])
            ageUnit = [ageLowUnit, ageHighUnit]
        sex = []
        for s in json.loads(row[13]):
            sex.append(dicom.getSexInMiabisFormat(s))
        return dict(id = row[0], name = row[1], previousId = row[2], 
                    authorId = row[3], authorName = row[4], authorEmail = row[5], 
                    creationDate = str(row[6]), description = row[7], public = row[8], 
                    studiesCount = row[9], subjectsCount = row[10], 
                    ageLow = ageLow, ageHigh = ageHigh, ageUnit = ageUnit, sex = sex, 
                    bodyPart = json.loads(row[14]), modality = json.loads(row[15]))

    def getStudiesFromDataset(self, datasetId, limit = 'ALL', skip = 0):
        self.cursor.execute(sql.SQL("""
            SELECT study.id, study.name, study.subject_name, study.path, study.url, dataset_study.series
            FROM study, dataset_study 
            WHERE dataset_study.dataset_id = %s AND dataset_study.study_id = study.id 
            ORDER BY study.name 
            LIMIT {} OFFSET {};""").format(sql.SQL(str(limit)), sql.SQL(str(skip))),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(dict(studyId = row[0], studyName = row[1], subjectName = row[2], 
                            path = row[3], series = json.loads(row[5]), url = row[4]))
        return res

    def getDatasets(self, skip, limit, onlyPublic, searchString):
        whereClause = sql.SQL("AND dataset.public = true" if onlyPublic else "")
        if searchString != '': 
            whereClause += sql.SQL(" AND dataset.name ILIKE {}").format(sql.Literal('%'+searchString+'%'))

        self.cursor.execute(sql.SQL("""
            SELECT dataset.id, dataset.name, author.name, dataset.creation_date, 
                   dataset.studies_count, dataset.subjects_count
            FROM dataset, author 
            WHERE dataset.author_id = author.id AND dataset.invalidated = false {}
            ORDER BY dataset.name 
            LIMIT {} OFFSET {};""").format(whereClause, sql.SQL(str(limit)), sql.SQL(str(skip)))
        )
        res = []
        for row in self.cursor:
            res.append(dict(id = row[0], name = row[1], authorName = row[2], creationDate = str(row[3]), 
                            studiesCount = row[4], subjectsCount = row[5]))
        return res
        
    def invalidateDataset(self, id):
        # logging.root.debug(self.cursor.mogrify("UPDATE dataset SET invalidated = true WHERE id = %s;", (id,)))
        self.cursor.execute("UPDATE dataset SET invalidated = true WHERE id = %s;", (id,))

    def createDatasetAccess(self, datasetAccessId, datasetIDs, userGID, toolName, toolVersion):
        self.cursor.execute("""
            INSERT INTO dataset_access (id, user_gid, tool_name, tool_version) 
            VALUES (%s, %s, %s, %s);""", 
            (datasetAccessId, userGID, toolName, toolVersion)
        )
        for id in datasetIDs:
            self.cursor.execute("""
                INSERT INTO dataset_access_dataset (dataset_access_id, dataset_id) 
                VALUES (%s, %s);""", 
                (datasetAccessId, id)
            )
