import logging
import psycopg2
from psycopg2 import sql
import json
from dataset_service import authorization, eucaim_formats, output_formats

class DB:
    def __init__(self, dbConfig):
        self.conn = psycopg2.connect(host=dbConfig.host, port=dbConfig.port, 
                                     dbname=dbConfig.dbname, user=dbConfig.user, password=dbConfig.password)
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

    CURRENT_SCHEMA_VERSION = 24

    def setup(self):
        version = self.getSchemaVersion()
        if version == 0:
            logging.root.info("Database is empty, creating tables...")
            self.createSchema()
            return
        else:
            logging.root.info("Current database schema version: %d " % version)
            if version > self.CURRENT_SCHEMA_VERSION:
                raise Exception("""The database schema version is greater than the last known by this app (%d). 
                                   Maybe you are trying to run an old app version over a database formated by a newer app version.
                                """ % self.CURRENT_SCHEMA_VERSION)
            if version < 2: self.updateDB_v1To2()
            if version < 3: self.updateDB_v2To3()
            if version < 4: self.updateDB_v3To4()
            if version < 5: self.updateDB_v4To5()
            if version < 6: self.updateDB_v5To6()
            if version < 7: self.updateDB_v6To7()
            if version < 10: self.updateDB_v7To10()
            if version < 11: self.updateDB_v10To11()
            if version < 12: self.updateDB_v11To12()
            if version < 13: self.updateDB_v12To13()
            if version < 14: self.updateDB_v13To14()
            if version < 15: self.updateDB_v14To15()
            if version < 16: self.updateDB_v15To16()
            if version < 17: self.updateDB_v16To17()
            if version < 18: self.updateDB_v17To18()
            if version < 19: self.updateDB_v18To19()
            if version < 20: self.updateDB_v19To20()
            if version < 21: self.updateDB_v20To21()
            if version < 22: self.updateDB_v21To22()
            if version < 23: self.updateDB_v22To23()
            if version < 24: self.updateDB_v23To24()
            ### Finally update schema_version
            self.cursor.execute("UPDATE metadata set schema_version = %d;" % self.CURRENT_SCHEMA_VERSION)

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
            VALUES ('%d')
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
                id varchar(40),
                name varchar(128) NOT NULL,
                subject_name varchar(128) NOT NULL,
                path_in_datalake varchar(256),
                url varchar(256),
                age_in_days integer DEFAULT NULL,
                sex char(1) DEFAULT NULL,
                diagnosis varchar(16) DEFAULT NULL,
                diagnosis_year integer DEFAULT NULL,
                study_date timestamp DEFAULT NULL,
                body_part varchar(16) DEFAULT NULL,
                modality varchar(16) DEFAULT NULL,
                manufacturer varchar(64) DEFAULT NULL,
                constraint pk_study primary key (id)
            );
            CREATE TABLE dataset (
                id varchar(40),
                name varchar(256) NOT NULL,
                previous_id varchar(40) DEFAULT NULL,
                next_id varchar(40) DEFAULT NULL,
                author_id varchar(64) NOT NULL,
                creation_date timestamp NOT NULL,
                description text NOT NULL DEFAULT '',
                license_title varchar(128) NOT NULL DEFAULT '',
                license_url varchar(256) NOT NULL DEFAULT '',
                pid_url varchar(256) DEFAULT NULL,
                zenodo_doi varchar(128) DEFAULT NULL,
                contact_info varchar(256) DEFAULT NULL,
                draft boolean NOT NULL DEFAULT true,
                public boolean NOT NULL DEFAULT false,
                invalidated boolean NOT NULL DEFAULT false,
                studies_count integer NOT NULL,
                subjects_count integer NOT NULL,
                age_low_in_days integer DEFAULT NULL,
                age_low_unit char(1) DEFAULT NULL,
                age_high_in_days integer DEFAULT NULL,
                age_high_unit char(1) DEFAULT NULL,
                age_null_count integer DEFAULT NULL,
                sex text NOT NULL DEFAULT '[]',
                sex_count text NOT NULL DEFAULT '[]',
                diagnosis_year_low integer DEFAULT NULL,
                diagnosis_year_high integer DEFAULT NULL,
                diagnosis_year_null_count integer DEFAULT NULL,
                body_part text NOT NULL DEFAULT '[]',
                body_part_count text NOT NULL DEFAULT '[]',
                modality text NOT NULL DEFAULT '[]',
                modality_count text NOT NULL DEFAULT '[]',
                manufacturer text NOT NULL DEFAULT '[]',
                manufacturer_count text NOT NULL DEFAULT '[]',
                series_tags text NOT NULL DEFAULT '[]',
                last_integrity_check timestamp DEFAULT NULL,
                constraint pk_dataset primary key (id),
                constraint fk_author foreign key (author_id) references author(id)
            );
            /* Every dataset has one of this during the creation; it is deleted when the creation successfully finish.
               The creation job writes here the status of the process, so the UI can inform to the user. */
            CREATE TABLE dataset_creation_status (
                dataset_id varchar(40),
                status varchar(10),
                last_message varchar(256),
                constraint pk_dataset_creation_status primary key (dataset_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id)
            );
            /* A dataset can contain multiple studies and a study can be contained in multiple datasets. */
            CREATE TABLE dataset_study (
                dataset_id varchar(40),
                study_id varchar(40),
                series text NOT NULL DEFAULT '[]',
                hash varchar(50) NOT NULL DEFAULT '',
                constraint pk_dataset_study primary key (dataset_id, study_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
                constraint fk_study foreign key (study_id) references study(id)
            );
            /* CREATE TABLE series (
                study_id varchar(40),
                seriesDirName varchar(128)
                hash varchar(50) NOT NULL DEFAULT '',
                hash_last_check timestamp DEFAULT NULL,
                constraint pk_series primary key (study_id, seriesDirName),
                constraint fk_study foreign key (study_id) references study(id)
            ); */
            /* Type options: 'i' (interactive desktop or web app), 
                             'b' (batch job)*/
            CREATE TABLE dataset_access (
                id varchar(40),
                user_gid integer,
                access_type char(1) DEFAULT '',
                tool_name varchar(256),
                tool_version varchar(256),
                image varchar(256) DEFAULT '',
                cmd_line varchar(512) DEFAULT '',
                resource_flavor varchar(32) DEFAULT '',
                openchallenge_job_type varchar(32) DEFAULT '',
                creation_time timestamp DEFAULT NULL,
                start_time timestamp DEFAULT NULL,
                end_time timestamp DEFAULT NULL,
                end_status varchar(32) DEFAULT '',
                closed boolean DEFAULT NULL,
                constraint pk_dataset_access primary key (id),
                constraint fk_user foreign key (user_gid) references author(gid)
            );
            CREATE TABLE dataset_access_dataset (
                dataset_access_id varchar(128),
                dataset_id varchar(40),
                constraint pk_dataset_access_dataset primary key (dataset_access_id, dataset_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id)
            );
            CREATE TABLE license (
                id SERIAL,
                name varchar(128) NOT NULL,
                url varchar(256),
                constraint pk_license primary key (id)
            );
        """ % self.CURRENT_SCHEMA_VERSION)
    
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

    def updateDB_v6To7(self):
        logging.root.info("Updating database from v6 to v7...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN license_url varchar(256) NOT NULL DEFAULT '';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN pid_url varchar(256) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN contact_info varchar(256) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN draft boolean NOT NULL DEFAULT true;")
    
    def updateDB_v7To10(self):
        logging.root.info("Updating database from v7 to v10...")
        self.cursor.execute("ALTER TABLE study ALTER COLUMN id TYPE varchar(40);")
        self.cursor.execute("ALTER TABLE dataset_study ALTER COLUMN study_id TYPE varchar(40);")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN sex TYPE varchar(16);")

    def updateDB_v10To11(self):
        logging.root.info("Updating database from v10 to v11...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN zenodo_doi varchar(128) DEFAULT NULL;")

    def updateDB_v11To12(self):
        logging.root.info("Updating database from v11 to v12...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN license_title varchar(128) NOT NULL DEFAULT '';")
        self.cursor.execute("""CREATE TABLE license (
                                    id SERIAL,
                                    name varchar(128) NOT NULL,
                                    url varchar(256),
                                    constraint pk_license primary key (id)
                               );""")
        self.cursor.execute("""INSERT INTO license (name, url) 
                               VALUES ('CC BY 4.0', 'https://creativecommons.org/licenses/by/4.0/')""")

    def updateDB_v12To13(self):
        logging.root.info("Updating database from v12 to v13...")
        self.cursor.execute("ALTER TABLE study RENAME COLUMN path TO path_in_datalake;")

    def updateDB_v13To14(self):
        logging.root.info("Updating database from v13 to v14...")
        self.cursor.execute("ALTER TABLE dataset_study ADD COLUMN hash varchar(50) NOT NULL DEFAULT '';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN series_tags text NOT NULL DEFAULT '[]';")

    def updateDB_v14To15(self):
        logging.root.info("Updating database from v14 to v15...")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN previous_id TYPE varchar(40);")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN next_id varchar(40) DEFAULT NULL;")
    
    def updateDB_v15To16(self):
        logging.root.info("Updating database from v15 to v16...")
        self.cursor.execute("""CREATE TABLE dataset_creation_status (
                                  dataset_id varchar(40),
                                  status varchar(10),
                                  last_message varchar(256),
                                  constraint pk_dataset_creation_status primary key (dataset_id),
                                  constraint fk_dataset foreign key (dataset_id) references dataset(id)
                               );""")

    def updateDB_v16To17(self):
        logging.root.info("Updating database from v16 to v17...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN last_integrity_check timestamp DEFAULT NULL;")
    
    def updateDB_v17To18(self):
        logging.root.info("Updating database from v17 to v18...")
        self.cursor.execute("ALTER TABLE study ADD COLUMN age_in_days integer DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN sex char(1) DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN body_part varchar(16) DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN modality varchar(16) DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN manufacturer varchar(64) DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN diagnosis varchar(16) DEFAULT NULL")
        self.cursor.execute("ALTER TABLE study ADD COLUMN study_date timestamp DEFAULT NULL")
    
    def updateDB_v18To19(self):
        logging.root.info("Updating database from v18 to v19...")
        self.cursor.execute("ALTER TABLE dataset DROP COLUMN age_low;")
        self.cursor.execute("ALTER TABLE dataset DROP COLUMN age_high;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_low_in_days integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_low_unit char(1) DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_high_in_days integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_high_unit char(1) DEFAULT NULL;")
    
    def updateDB_v19To20(self):
        logging.root.info("Updating database from v19 to v20...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN age_null_count integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN sex_count text NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN body_part_count text NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN modality_count text NOT NULL DEFAULT '[]';")
    
    def updateDB_v20To21(self):
        logging.root.info("Updating database from v20 to v21...")
        self.cursor.execute("ALTER TABLE study ADD COLUMN diagnosis_year integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN diagnosis_year_low integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN diagnosis_year_high integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN diagnosis_year_null_count integer DEFAULT NULL;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN manufacturer text NOT NULL DEFAULT '[]';")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN manufacturer_count text NOT NULL DEFAULT '[]';")

    def updateDB_v21To22(self):
        logging.root.info("Updating database from v21 to v22...")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN sex TYPE text;")

    def updateDB_v22To23(self):
        logging.root.info("Updating database from v22 to v23...")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN access_type char(1) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN image varchar(256) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN cmd_line varchar(512) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN start_time timestamp DEFAULT NULL")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN end_time timestamp DEFAULT NULL")

    def updateDB_v23To24(self):
        logging.root.info("Updating database from v23 to v24...")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN resource_flavor varchar(32) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN openchallenge_job_type varchar(32) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN creation_time timestamp DEFAULT NULL")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN end_status varchar(32) DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset_access ADD COLUMN closed boolean DEFAULT NULL")


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
    
    def createOrUpdateUser(self, userId, username, groups, gid = None):
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
            # delete and reintroduce the groups because they could be changed
            self.cursor.execute("DELETE FROM user_group WHERE user_id=%s;", (userId,))
        for group in groups:
            self.cursor.execute("""
                INSERT INTO user_group (user_id, group_name) 
                VALUES (%s, %s)
                ON CONFLICT (user_id, group_name) DO NOTHING;""", 
                (userId, group))

    def getUserIDs(self, userName):
        self.cursor.execute("SELECT id, gid FROM author WHERE username=%s LIMIT 1;", (userName,))
        row = self.cursor.fetchone()
        if row is None: return None, None
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
                                 studies_count, subjects_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
            (dataset["id"], dataset["name"], dataset["previousId"], userId, 
             dataset["creationDate"], dataset["description"], dataset["public"], 
             dataset["studiesCount"], dataset["subjectsCount"]))

    def updateDatasetMetadata(self, dataset):
        # For now let's store directly the final format for each value in the properties of type array of strings,
        # because json don't allow store the None value in a array of strings.
        # In final format the None value is converted to "Unknown" which is compliant with Miabis.
        sexList = [output_formats.sexToMiabis(i) for i in dataset["sex"]]
        bodyPartList = [output_formats.bodyPartToOutputFormat(i) for i in dataset["bodyPart"]]
        modalityList = [output_formats.modalityToOutputFormat(i) for i in dataset["modality"]]
        manufacturerList = [output_formats.manufacturerToOutputFormat(i) for i in dataset["manufacturer"]]
        self.cursor.execute("""UPDATE dataset 
                               SET studies_count = %s, subjects_count = %s, 
                                   age_low_in_days = %s, age_low_unit = %s, 
                                   age_high_in_days = %s, age_high_unit = %s, 
                                   age_null_count = %s, 
                                   sex = %s, sex_count = %s, 
                                   diagnosis_year_low = %s, diagnosis_year_high = %s, 
                                   diagnosis_year_null_count = %s, 
                                   body_part = %s, body_part_count = %s, 
                                   modality = %s, modality_count = %s, 
                                   manufacturer = %s, manufacturer_count = %s, 
                                   series_tags = %s
                               WHERE id = %s;""", 
                            (dataset["studiesCount"], dataset["subjectsCount"], 
                             dataset["ageLowInDays"], dataset["ageLowUnit"], 
                             dataset["ageHighInDays"], dataset["ageHighUnit"], 
                             dataset["ageNullCount"], 
                             json.dumps(sexList), json.dumps(dataset["sexCount"]), 
                             dataset["diagnosisYearLow"], dataset["diagnosisYearHigh"], 
                             dataset["diagnosisYearNullCount"], 
                             json.dumps(bodyPartList), json.dumps(dataset["bodyPartCount"]), 
                             json.dumps(modalityList), json.dumps(dataset["modalityCount"]), 
                             json.dumps(manufacturerList), json.dumps(dataset["manufacturerCount"]), 
                             json.dumps(dataset["seriesTags"]), 
                             dataset["id"]))

    def updateStudyMetadata(self, study):
        self.cursor.execute("""UPDATE study
                               SET age_in_days = %s, sex = %s, body_part = %s, modality = %s, 
                                   manufacturer = %s, diagnosis = %s, diagnosis_year = %s, study_date = %s 
                               WHERE id = %s;""", 
                            (study['ageInDays'], study['sex'], study['bodyPart'], study['modality'], 
                             study['manufacturer'], study['diagnosis'], study['diagnosisYear'], study['studyDate'],
                             study['studyId']))

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

    def existDataset(self, id):
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
                   dataset.next_id, dataset.last_integrity_check
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
        
        return dict(id = row[0], name = row[1], 
                    previousId = row[2], nextId = row[35], 
                    authorId = row[3], authorName = row[4], authorEmail = row[5], 
                    creationDate = creationDate, description = row[7], 
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
                    studiesCount = row[16], subjectsCount = row[17], 
                    ageLow = ageLow, ageHigh = ageHigh, ageUnit = ageUnit, ageNullCount = row[22], 
                    sex = json.loads(row[23]), sexCount = json.loads(row[24]), 
                    diagnosisYearLow = row[25], diagnosisYearHigh = row[26], diagnosisYearNullCount = row[27], 
                    bodyPart = json.loads(row[28]), bodyPartCount = json.loads(row[29]), 
                    modality = json.loads(row[30]), modalityCount = json.loads(row[31]), 
                    manufacturer = json.loads(row[32]), manufacturerCount = json.loads(row[33]), 
                    seriesTags = json.loads(row[34]), lastIntegrityCheck = lastIntegrityCheck)

    def getStudiesFromDataset(self, datasetId, limit = 0, skip = 0):
        if limit == 0: limit = 'ALL'

        # First get total rows without LIMIT and OFFSET
        self.cursor.execute("SELECT count(*) FROM dataset_study WHERE dataset_study.dataset_id = %s", (datasetId,))
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0

        self.cursor.execute(sql.SQL("""
            SELECT study.id, study.name, study.subject_name, study.url, study.path_in_datalake, 
                   dataset_study.series, dataset_study.hash
            FROM study, dataset_study 
            WHERE dataset_study.dataset_id = %s AND dataset_study.study_id = study.id 
            ORDER BY study.name 
            LIMIT {} OFFSET {};""").format(sql.SQL(str(limit)), sql.SQL(str(skip))),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(dict(studyId = row[0], studyName = row[1], subjectName = row[2], pathInDatalake = row[4],
                            series = json.loads(row[5]), url = row[3], hash = row[6]))
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
                    sortBy = 'creationDate', sortDirection = '', searchSubject: str = ''):
        fromExtra = sql.Composed([])
        whereClause = sql.Composed([])

        if searchFilter.public != None:
            whereClause += sql.SQL(" AND dataset.public = ") + sql.Literal(searchFilter.public)

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
                SELECT dataset.id, dataset.name, author.name, dataset.creation_date, 
                    dataset.draft, dataset.public, dataset.invalidated, 
                    dataset.studies_count, dataset.subjects_count
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
            res.append(dict(id = row[0], name = row[1], authorName = row[2], creationDate = creationDate, 
                            draft = row[4], public = row[5], invalidated = row[6],
                            studiesCount = row[7], subjectsCount = row[8]))
        return res, total
    
    def getUpgradableDatasets(self, filter: authorization.Upgradables_filter):
        whereClause = sql.SQL("")
        if filter.getUserId() != None:
            authorId = sql.Literal(str(filter.getUserId()))
            whereClause = sql.SQL(" AND author_id = ") + authorId
        self.cursor.execute(sql.SQL("""
            SELECT id, name
            FROM dataset
            WHERE draft = false AND next_id is NULL {}
            ORDER BY name;""").format(whereClause))
        res = []
        for row in self.cursor:
            res.append(dict(id = row[0], name = row[1]))
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

    class searchValidationException(Exception): pass

    @staticmethod
    def _ensureIsStr(value, conditionType, conditionKey):
        if not isinstance(value, str): 
            raise DB.searchValidationException(
                "'value' for type '%s' in condition for %s must be a string" % (conditionType, conditionKey))
    @staticmethod
    def _ensureIsNum(value, conditionType, conditionKey):
        if not isinstance(value, (int, float)): 
            raise DB.searchValidationException(
                "'value' for type '%s' in condition for %s must be a number" % (conditionType, conditionKey))
    @staticmethod
    def _ensureIsRangeOfNum(value, conditionType, conditionKey):
        if not isinstance(value, dict): 
            raise DB.searchValidationException("value for type '%s' in condition for %s must be an object" % (conditionType, conditionKey))
        if not 'min' in value: raise DB.searchValidationException("missing 'min' in range")
        if not 'max' in value: raise DB.searchValidationException("missing 'max' in range")
        if not isinstance(value["min"], (int, float)) or not isinstance(value["max"], (int, float)): 
                raise DB.searchValidationException("values for range condition for %s must be numbers" % conditionKey)
    @staticmethod
    def _ensureIsArrayOfString(value, conditionType, conditionKey):
        if not isinstance(value, list): 
            raise DB.searchValidationException("value for type '%s' in condition for %s must be an array of strings" % (conditionType, conditionKey))
        for s in value:
            if not isinstance(s, str): 
                raise DB.searchValidationException("value for type '%s' in condition for %s must be an array of strings" % (conditionType, conditionKey))

    @staticmethod
    def _searchConditionStringValueToSQL(key, type, value) -> sql.SQL:
        if type == "EQUALS":
            if value is None:
                return sql.SQL("{} IS NULL").format(sql.SQL(key))
            else: return sql.SQL("{} = {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "NOT_EQUALS":
            if value is None:
                return sql.SQL("NOT {} IS NULL").format(sql.SQL(key), sql.Literal(value))
            else: return sql.SQL("{} <> {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "IN":
            res = sql.SQL("")
            if value.count(None) > 0:
                res += sql.SQL("{} IS NULL").format(sql.SQL(key))
                value.remove(None)
                if len(value) == 0: return res
                res += sql.SQL(" OR ")
            res += sql.SQL("{} IN ({})").format(sql.SQL(key), sql.SQL(', ').join(sql.Literal(item) for item in value))
            return res
        elif type == "CONTAINS":
            return sql.SQL("{} ILIKE {}").format(sql.SQL(key), sql.Literal('%'+value+'%'))
        else: 
            raise DB.searchValidationException("unknown 'type' in condition for %s" % key)

    @staticmethod
    def _searchConditionNumValueToSQL(key, type, value) -> sql.SQL:
        if type == "EQUALS":
            return sql.SQL("{} = {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "NOT_EQUALS":
            return sql.SQL("{} <> {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "BETWEEN":
            return sql.SQL("{} BETWEEN {} AND {}").format(sql.SQL(key), sql.Literal(value["min"]), sql.Literal(value["max"]))
        elif type == "LOWER_THAN":
            return sql.SQL("{} < {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "LOWER_EQUAL_THAN":
            return sql.SQL("{} <= {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "GREATER_THAN":
            return sql.SQL("{} > {}").format(sql.SQL(key), sql.Literal(value))
        elif type == "GREATER_EQUAL_THAN":
            return sql.SQL("{} >= {}").format(sql.SQL(key), sql.Literal(value))
        else: 
            raise DB.searchValidationException("unknown 'type' in condition for %s" % key)

    @staticmethod
    def _searchConditionStringToSQL(type, value, key, db_column, translate) -> sql.SQL:
        if type in ["IN"]:
            DB._ensureIsArrayOfString(value, type, key)
            try:
                value = [translate(s) for s in value]
            except Exception as e: raise DB.searchValidationException("unknown 'value' in condition for %s" % key)
        elif type in ["EQUALS","NOT_EQUALS","CONTAINS"]:
            DB._ensureIsStr(value, type, key)
            try:
                value = translate(value)
            except Exception as e: raise DB.searchValidationException("unknown 'value' in condition for %s" % key)
        else: raise DB.searchValidationException("unknown 'type' in condition for %s" % key)
        return DB._searchConditionStringValueToSQL(db_column, type, value)

    @staticmethod
    def _searchConditionNumToSQL(type, value, key, db_column, translate) -> sql.SQL:
        if type in ["BETWEEN"]:
            DB._ensureIsRangeOfNum(value, type, key)
            value["min"] = translate(value["min"])
            value["max"] = translate(value["max"])
        elif type in ["LOWER_THAN","GREATER_THAN"]:
            if isinstance(value, dict): 
                # strange case but accepted if value is a range, just one of the limits will be taken
                DB._ensureIsRangeOfNum(value, type, key)
                if type == "LOWER_THAN": 
                    value = value["max"]  # ignore min
                    type = "LOWER_EQUAL_THAN"
                else:  # type == "GREATER_THAN"
                    value = value["min"]  # ignore max
                    type = "GREATER_EQUAL_THAN"
            else:
                DB._ensureIsNum(value, type, key)
            value = translate(value)
        elif type in ["EQUALS","NOT_EQUALS"]:
            DB._ensureIsNum(value, type, key)
            value = translate(value)
        else: raise DB.searchValidationException("unknown 'type' in condition for %s" % key)
        return DB._searchConditionNumValueToSQL(db_column, type, value)

    @staticmethod
    def _searchRequestToSQL(sr) -> sql.SQL | None:
        if 'operand' in sr:   # is an operation
            if not sr['operand'] in ['AND', 'OR']: raise DB.searchValidationException("unknown value for 'operand'")
            if not 'children' in sr: raise DB.searchValidationException("missing 'children' in operation")
            if not isinstance(sr['children'], list): raise DB.searchValidationException("'children' in operation must be an array")
            if len(sr['children']) == 0: return None
            operation = sql.SQL(' %s ' % sr['operand']).join(DB._searchRequestToSQL(child) for child in sr['children'])
            return sql.SQL("(")+operation+sql.SQL(")")
        elif 'key' in sr:   # is a condition
            if not 'type' in sr: raise DB.searchValidationException("missing 'type' in condition")
            if not 'value' in sr: raise DB.searchValidationException("missing 'value' in condition")
            if sr['key'] == 'SNOMEDCT263495000':  # gender
                res = DB._searchConditionStringToSQL(sr['type'], sr['value'], 'gender', 'study.sex', eucaim_formats.getGender)
            elif sr['key'] == 'SNOMEDCT423493009':  # age at diagnosis
                res = DB._searchConditionNumToSQL(sr['type'], sr['value'], 'age', 'study.age_in_days', eucaim_formats.getAge)
            elif sr['key'] == 'SNOMEDCT439401001':  # diagnosis
                res = DB._searchConditionStringToSQL(sr['type'], sr['value'], 'diagnosis', 'study.diagnosis', eucaim_formats.getDiagnosis)
            elif sr['key'] == 'SNOMEDCT432213005':  # year_of_diagnosis
                res = DB._searchConditionNumToSQL(sr['type'], sr['value'], 'year of diagnosis', 'study.diagnosis_year', eucaim_formats.getYear)
            elif sr['key'] == 'RID10311':  # modality   SNOMEDCT363679005
                res = DB._searchConditionStringToSQL(sr['type'], sr['value'], 'modality', 'study.modality', eucaim_formats.getModality)
            elif sr['key'] == 'SNOMEDCT123037004':  # body part   # mejor SNOMEDCT38866009 ?
                res = DB._searchConditionStringToSQL(sr['type'], sr['value'], 'body part', 'study.body_part', eucaim_formats.getBodyPart)
            elif sr['key'] == 'C25392':  # manufacturer
                res = DB._searchConditionStringToSQL(sr['type'], sr['value'], 'Manufacturer', 'study.manufacturer', eucaim_formats.getManufacturer)
            else: raise DB.searchValidationException("unkown 'key' in condition")
            return sql.SQL("(")+res+sql.SQL(")")
        else: raise DB.searchValidationException("missing 'operand' or 'key'")

    def eucaimSearchDatasets(self, skip, limit, searchRequest):
        whereClause = DB._searchRequestToSQL(searchRequest)
        whereClause = sql.SQL("") if whereClause is None else sql.SQL("AND ") + whereClause
        if limit == 0: limit = 'ALL'
        q = sql.SQL("""
                SELECT dataset.id, dataset.name, dataset.creation_date, 
                    dataset.draft, dataset.public, dataset.invalidated, 
                    COUNT(study.id), COUNT(DISTINCT study.subject_name), 
                    dataset.age_low_in_days, dataset.age_high_in_days, dataset.sex, 
                    dataset.modality, dataset.body_part, dataset.description
                FROM dataset, dataset_study, study
                WHERE dataset.id = dataset_study.dataset_id AND dataset_study.study_id = study.id
                      AND dataset.draft = false AND dataset.invalidated = false {}
                GROUP BY dataset.id
                ORDER BY dataset.creation_date DESC
                LIMIT {} OFFSET {};"""
            ).format(whereClause, sql.SQL(str(limit)), sql.SQL(str(skip)))
        logging.root.debug("QUERY: " + q.as_string(self.conn))
        self.cursor.execute(q)
        res = []
        for row in self.cursor:
            res.append(dict(id = row[0], name = row[1], 
                            studies_count = row[6], subjects_count = row[7], 
                            age_range = dict(min = round(row[8]/365) if row[8] != None else 0, 
                                             max = round(row[9]/365)) if row[9] != None else 0,
                            gender = json.loads(row[10]), 
                            modality = json.loads(row[11]), 
                            body_parts = json.loads(row[12]),
                            description = row[13]))
        return res

    def deleteDataset(self, datasetId):
        self.cursor.execute("DELETE FROM dataset_creation_status WHERE dataset_id=%s;", (datasetId,))
        self.cursor.execute("DELETE FROM dataset_study WHERE dataset_id=%s;", (datasetId,))
        self.cursor.execute("DELETE FROM dataset WHERE id=%s;", (datasetId,))

    def deleteOrphanStudies(self):
        '''This is a kind of garbage-collection that deletes all the studies not included in any dataset_study.'''
        self.cursor.execute("""
            SELECT COUNT(*) FROM study as s
            WHERE not exists (select ds.dataset_id, ds.study_id 
                              from dataset_study as ds
                              where ds.study_id = s.id);""")
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0
        if total == 0:
            logging.root.debug("There are no orphan studies to remove (all of them were included in other datasets).")
            return
        logging.root.debug("There are %d orphan studies (not included in any dataset), removing..." % total )
        self.cursor.execute("""
            DELETE FROM study as s
            WHERE not exists (select ds.dataset_id, ds.study_id 
                              from dataset_study as ds
                              where ds.study_id = s.id);""")

    def getLicenses(self):
        self.cursor.execute("""
            SELECT name, url
            FROM license
            ORDER BY name;""")
        res = []
        for row in self.cursor:
            res.append(dict(title = row[0], url = row[1]))
        return res

    def setZenodoDOI(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET zenodo_doi = %s WHERE id = %s;", (newValue, id))

    def setDatasetInvalidated(self, id, newValue):
        # logging.root.debug(self.cursor.mogrify("UPDATE dataset SET invalidated = true WHERE id = %s;", (id,)))
        self.cursor.execute("UPDATE dataset SET invalidated = %s WHERE id = %s;", (newValue, id))

    def setDatasetPublic(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET public = %s WHERE id = %s;", (newValue, id))
        
    def setDatasetDraft(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET draft = %s WHERE id = %s;", (newValue, id))

    def setDatasetName(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET name = %s WHERE id = %s;", (newValue, id))

    def setDatasetDescription(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET description = %s WHERE id = %s;", (newValue, id))

    def setDatasetPreviousId(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET previous_id = %s WHERE id = %s;", (newValue, id))

    def setDatasetNextId(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET next_id = %s WHERE id = %s;", (newValue, id))

    def setDatasetLicense(self, id, newTitle, newUrl):
        self.cursor.execute("UPDATE dataset SET license_title = %s, license_url = %s WHERE id = %s;", 
                            (newTitle, newUrl, id))

    def setDatasetPid(self, id, preferred, custom = None):
        newValue = self.PREFERRED_ZENODO if preferred == "zenodoDoi" else custom
        self.cursor.execute("UPDATE dataset SET pid_url = %s WHERE id = %s;", (newValue, id))

    def setDatasetContactInfo(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET contact_info = %s WHERE id = %s;", (newValue, id))

    def setDatasetLastIntegrityCheck(self, id, newValue):
        self.cursor.execute("UPDATE dataset SET last_integrity_check = %s WHERE id = %s;", (newValue, id))

    def createDatasetAccess(self, datasetAccessId, datasetIDs, userGID, accessType, toolName, toolVersion, image, cmdLine, creationTime, resourcesFlavor, openchallengeJobType):
        self.cursor.execute("""
            INSERT INTO dataset_access (id, user_gid, access_type, tool_name, tool_version, image, cmd_line, creation_time, resource_flavor, openchallenge_job_type, closed) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE);""", 
            (datasetAccessId, userGID, accessType, toolName, toolVersion, image, cmdLine, creationTime, resourcesFlavor, openchallengeJobType)
        )
        for id in datasetIDs:
            self.cursor.execute("""
                INSERT INTO dataset_access_dataset (dataset_access_id, dataset_id) 
                VALUES (%s, %s);""", 
                (datasetAccessId, id)
            )

    def existDatasetAccess(self, datasetAccessId):
        self.cursor.execute("SELECT id FROM dataset_access WHERE id=%s", (datasetAccessId,))
        return self.cursor.rowcount > 0

    def getDatasetAccess(self, datasetAccessId):
        self.cursor.execute("""
            SELECT dataset_access.user_gid, dataset_access_dataset.dataset_id
            FROM dataset_access, dataset_access_dataset
            WHERE dataset_access.id = %s
                  AND dataset_access.id = dataset_access_dataset.dataset_access_id;""", 
            (datasetAccessId,))
        datasetIDs = []
        userGID = None
        for row in self.cursor:
            userGID = row[0]  # the same in all rows
            datasetIDs.append(row[1])
        return userGID, datasetIDs

    def getDatasetsCurrentlyAccessedByUser(self, userGID):
        self.cursor.execute("""
            SELECT dataset_access_dataset.dataset_id
            FROM dataset_access, dataset_access_dataset
            WHERE dataset_access.user_gid = %s
                  AND dataset_access.closed IS NOT TRUE
                  AND dataset_access.id = dataset_access_dataset.dataset_access_id;""", 
            (userGID,))
        datasetIDs = []
        for row in self.cursor:
            datasetIDs.append(row[0])
        return datasetIDs

    def getOpenDatasetAccesses(self, datasetId):
        self.cursor.execute("""
            SELECT author.username, dataset_access.tool_name, dataset_access.tool_version, dataset_access.id
            FROM dataset_access, dataset_access_dataset, author
            WHERE dataset_access_dataset.dataset_id = %s
                  AND dataset_access_dataset.dataset_access_id = dataset_access.id 
                  AND dataset_access.closed IS NOT TRUE
                  AND dataset_access.user_gid = author.gid;""", (datasetId,))
        res = []
        for row in self.cursor:
            res.append(dict(username = row[0], toolName = row[1], toolVersion = row[2], datasetAccessId = row[3]))
        return res

    def deleteDatasetAccess(self, datasetAccessId):
        self.cursor.execute("DELETE FROM dataset_access_dataset WHERE dataset_access_id=%s;", (datasetAccessId,))
        self.cursor.execute("DELETE FROM dataset_access WHERE id=%s;", (datasetAccessId,))
        
    def endDatasetAccess(self, datasetAccessId, startTime, endTime, endStatus):
        self.cursor.execute("""
            UPDATE dataset_access set start_time=%s, end_time=%s, end_status=%s, closed=TRUE
            WHERE id=%s;""",
            (startTime, endTime, endStatus, datasetAccessId))


