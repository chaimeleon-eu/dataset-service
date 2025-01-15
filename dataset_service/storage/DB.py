import logging
import psycopg2
import json

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

    CURRENT_SCHEMA_VERSION = 35

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
            if version < 30: 
                raise Exception("""The database schema version is too old to update. 
                                   You should launch a transitional run of any version of the service from 3.6 to 3.14
                                   just to update the schema to a more recent version (>=30) known by this version of the service.""")
            if version < 31: self.updateDB_v30To31()
            if version < 32: self.updateDB_v31To32()
            if version < 33: self.updateDB_v32To33()
            if version < 34: self.updateDB_v33To34()
            if version < 35: self.updateDB_v34To35()
            ### Finally update schema_version
            self.cursor.execute("UPDATE metadata set schema_version = %d;" % self.CURRENT_SCHEMA_VERSION)

    def getSchemaVersion(self):
        self.cursor.execute("SELECT FROM information_schema.tables WHERE table_name = 'metadata'")
        row = self.cursor.fetchone()
        if row is None: return 0
        self.cursor.execute("SELECT schema_version FROM metadata limit 1")
        row = self.cursor.fetchone()
        if row is None: raise Exception()
        return row[0]

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
            CREATE TABLE dataset (
                id varchar(40),
                name varchar(256) NOT NULL,
                version varchar(16) NOT NULL DEFAULT '',
                project_code varchar(80) NOT NULL,
                previous_id varchar(40) DEFAULT NULL,
                next_id varchar(40) DEFAULT NULL,
                author_id varchar(64) NOT NULL,
                creation_date timestamp NOT NULL,
                description text NOT NULL DEFAULT '',
                provenance text NOT NULL DEFAULT '',
                purpose text NOT NULL DEFAULT '',
                type varchar(16) ARRAY NOT NULL DEFAULT ARRAY[]::varchar[],
                collection_method varchar(16) ARRAY NOT NULL DEFAULT ARRAY[]::varchar[],
                license_title varchar(128) NOT NULL DEFAULT '',
                license_url varchar(256) NOT NULL DEFAULT '',
                pid_url varchar(256) DEFAULT NULL,
                zenodo_doi varchar(128) DEFAULT NULL,
                contact_info varchar(256) DEFAULT NULL,
                draft boolean NOT NULL DEFAULT true,
                public boolean NOT NULL DEFAULT false,
                invalidated boolean NOT NULL DEFAULT false,
                invalidation_reason varchar(128) DEFAULT NULL, 
                corrupted boolean NOT NULL DEFAULT false,
                studies_count integer NOT NULL,
                subjects_count integer NOT NULL,
                age_low_in_days integer DEFAULT NULL,
                age_low_unit char(1) DEFAULT NULL,
                age_high_in_days integer DEFAULT NULL,
                age_high_unit char(1) DEFAULT NULL,
                age_null_count integer DEFAULT NULL,
                sex text NOT NULL DEFAULT '[]',
                sex_count text NOT NULL DEFAULT '[]',
                diagnosis text NOT NULL DEFAULT '[]',
                diagnosis_count text NOT NULL DEFAULT '[]',
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
                size_in_bytes bigint DEFAULT NULL,
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
            /* Allowed users to access to a dataset apart from the user joined to the project. */
            CREATE TABLE dataset_acl (
                dataset_id varchar(40),
                user_id varchar(64),
                constraint pk_dataset_access_control primary key (dataset_id, user_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
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
                constraint pk_study primary key (id)
            );
            /* A dataset can contain multiple studies and a study can be contained in multiple datasets. */
            CREATE TABLE dataset_study (
                dataset_id varchar(40),
                study_id varchar(40),
                series text NOT NULL DEFAULT '[]',
                hash varchar(50) NOT NULL DEFAULT '',
                size_in_bytes bigint DEFAULT NULL,
                constraint pk_dataset_study primary key (dataset_id, study_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
                constraint fk_study foreign key (study_id) references study(id)
            );
            CREATE TABLE series (
                study_id varchar(40),
                folder_name varchar(128),
                body_part varchar(16) DEFAULT NULL,
                modality varchar(16) DEFAULT NULL,
                manufacturer varchar(64) DEFAULT NULL,
                hash_cache bytea DEFAULT NULL,
                hash_last_time_calculated timestamp DEFAULT NULL,
                constraint pk_series primary key (study_id, folder_name),
                constraint fk_study foreign key (study_id) references study(id)
            );
            CREATE TABLE dataset_study_series (
                dataset_id varchar(40),
                study_id varchar(40),
                series_folder_name varchar(128),
                constraint pk_dataset_study_series primary key (dataset_id, study_id, series_folder_name),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
                constraint fk_study foreign key (study_id) references study(id),
                constraint fk_series foreign key (study_id, series_folder_name) references series(study_id, folder_name)
            );
            
            /* access_type options: 'i' (interactive desktop or web app), 
                                    'b' (batch job) */
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
                constraint pk_license primary key (id),
                constraint un_name unique (name)
            );
            INSERT INTO license (name, url) 
                VALUES ('CC BY 4.0', 'https://creativecommons.org/licenses/by/4.0/');
            
            CREATE TABLE project (
                code varchar(16),
                name varchar(128) NOT NULL,
                short_description varchar(512) NOT NULL,
                external_url varchar(256) NOT NULL DEFAULT '',
                logo_file_name varchar(64) NOT NULL DEFAULT '',
                default_contact_info varchar(256) NOT NULL DEFAULT '',
                default_license_title varchar(128) NOT NULL DEFAULT '',
                default_license_url varchar(256) NOT NULL DEFAULT '',
                zenodo_access_token varchar(128) NOT NULL DEFAULT '',
                zenodo_author varchar(128) NOT NULL DEFAULT '',
                zenodo_community varchar(128) NOT NULL DEFAULT '',
                zenodo_grant varchar(128) NOT NULL DEFAULT '',
                constraint pk_project primary key (code)
            );
        """ % self.CURRENT_SCHEMA_VERSION)
    
#region =================== Version update functions

    def updateDB_v30To31(self):
        logging.root.info("Updating database from v30 to v31...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN corrupted boolean NOT NULL DEFAULT false")

    def updateDB_v31To32(self):
        logging.root.info("Updating database from v31 to v32...")
        self.cursor.execute("DROP TABLE user_group;")
        self.cursor.execute("ALTER TABLE license ADD constraint un_name unique (name);")

    def updateDB_v32To33(self):
        logging.root.info("Updating database from v32 to v33...")
        self.cursor.execute("ALTER TABLE series ADD COLUMN hash_cache bytea DEFAULT NULL")
        self.cursor.execute("ALTER TABLE series ADD COLUMN hash_last_time_calculated timestamp DEFAULT NULL")

    def updateDB_v33To34(self):
        logging.root.info("Updating database from v33 to v34...")
        self.cursor.execute("""CREATE TABLE project (
                code varchar(16),
                name varchar(128) NOT NULL,
                short_description varchar(512) NOT NULL,
                external_url varchar(256) NOT NULL DEFAULT '',
                logo_file_name varchar(64) NOT NULL DEFAULT '',
                default_contact_info varchar(256) NOT NULL DEFAULT '',
                default_license_title varchar(128) NOT NULL DEFAULT '',
                default_license_url varchar(256) NOT NULL DEFAULT '',
                zenodo_access_token varchar(128) NOT NULL DEFAULT '',
                zenodo_author varchar(128) NOT NULL DEFAULT '',
                zenodo_community varchar(128) NOT NULL DEFAULT '',
                zenodo_grant varchar(128) NOT NULL DEFAULT '',
                constraint pk_project primary key (code)
            );""")
    
    def updateDB_v34To35(self):
        logging.root.info("Updating database from v34 to v35...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN provenance text NOT NULL DEFAULT ''")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN diagnosis text NOT NULL DEFAULT '[]'")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN diagnosis_count text NOT NULL DEFAULT '[]'")
    
#endregion
