import logging
import psycopg2
from psycopg2 import sql

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

        if version ==1: self.updateDB_v1To2()

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
            CREATE TABLE IF NOT EXISTS metadata (
                id integer DEFAULT 1 NOT NULL CHECK (id = 1),
                schema_version integer NOT NULL,
                constraint pk_metadata primary key (id)
            );
            INSERT INTO metadata (schema_version) 
            VALUES ('2')
            ON CONFLICT (id) DO UPDATE
                SET schema_version = excluded.schema_version;
                
            CREATE TABLE IF NOT EXISTS author (
                id varchar(64),
                name varchar(128),
                email varchar(128),
                constraint pk_user primary key (id)
            );
            CREATE TABLE IF NOT EXISTS study (
                id varchar(32),
                name varchar(128) NOT NULL,
                subject_name varchar(128) NOT NULL,
                path varchar(256),
                url varchar(256),
                constraint pk_study primary key (id)
            );
            CREATE TABLE IF NOT EXISTS dataset (
                id varchar(40),
                name varchar(256) NOT NULL,
                previous_id varchar(32) DEFAULT NULL,
                author_id varchar(64) NOT NULL,
                creation_date timestamp NOT NULL,
                description text NOT NULL DEFAULT '',
                gid integer NOT NULL,
                public boolean NOT NULL DEFAULT false,
                invalidated boolean NOT NULL DEFAULT false,
                studies_count integer NOT NULL,
                patients_count integer NOT NULL,
                constraint pk_dataset primary key (id),
                constraint fk_author foreign key (author_id) references author(id)
            );
            /* A dataset can contain multiple studies and a study can be contained in multiple datasets. */
            CREATE TABLE IF NOT EXISTS dataset_study (
                dataset_id varchar(40),
                study_id varchar(32),
                constraint pk_dataset_study primary key (dataset_id, study_id),
                constraint fk_dataset foreign key (dataset_id) references dataset(id),
                constraint fk_study foreign key (study_id) references study(id)
            );
        """)
    
    def updateDB_v1To2(self):
        logging.root.info("Updating database from v1 to v2...")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN invalidated boolean NOT NULL DEFAULT false;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN studies_count integer NOT NULL DEFAULT 0;")
        self.cursor.execute("ALTER TABLE dataset ADD COLUMN patients_count integer NOT NULL DEFAULT 0;")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN studies_count DROP DEFAULT;")
        self.cursor.execute("ALTER TABLE dataset ALTER COLUMN patients_count DROP DEFAULT;")
        self.cursor.execute("UPDATE metadata set schema_version = 2;")

    def createOrUpdateAuthor(self, userId, name, email):
        self.cursor.execute("""
            INSERT INTO author (id, name, email) 
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE
                SET name = excluded.name,
                    email = excluded.email;""", 
            (userId, name, email)
        )
    
    
    def createDataset(self, dataset, userId):
        self.cursor.execute("""
            INSERT INTO dataset (id, name, previous_id, author_id, creation_date, description, gid, public,
                                 studies_count, patients_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
            (dataset["id"], dataset["name"], dataset["previousId"], 
             userId, dataset["creationDate"], dataset["description"], 
             dataset["gid"], dataset["public"],
             dataset["studiesCount"], dataset["patientsCount"]))

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
            INSERT INTO dataset_study (dataset_id, study_id)
            VALUES (%s,%s);""",
            (datasetId, study["studyId"]))

    def existDataset(self, id):
        self.cursor.execute("SELECT id FROM dataset WHERE id=%s", (id,))
        return self.cursor.rowcount > 0

    def getDataset(self, id):
        self.cursor.execute("""
            SELECT dataset.id, dataset.name, dataset.previous_id, 
                   author.id, author.name, author.email, 
                   dataset.creation_date, dataset.description, dataset.gid, dataset.public,
                   dataset.studies_count, dataset.patients_count
            FROM dataset, author 
            WHERE dataset.id=%s AND author.id = dataset.author_id AND dataset.invalidated = false
            LIMIT 1;""",
            (id,))
        row = self.cursor.fetchone()
        if row == None: return None
        return dict(id = row[0], name = row[1], previousId = row[2], 
                    authorId = row[3], authorName = row[4], authorEmail = row[5], 
                    creationDate = str(row[6]), description = row[7], gid = row[8], public = row[9],
                    studiesCount = row[10], patientsCount = row[11])

    def getStudiesFromDataset(self, datasetId, limit = 'ALL', skip = 0):
        self.cursor.execute(sql.SQL("""
            SELECT study.id, study.name, study.subject_name, study.path, study.url
            FROM study, dataset_study 
            WHERE dataset_study.dataset_id = %s AND dataset_study.study_id = study.id
            LIMIT {} OFFSET {};""").format(sql.SQL(str(limit)), sql.SQL(str(skip))),
            (datasetId,)
        )
        res = []
        for row in self.cursor:
            res.append(dict(studyId = row[0], studyName = row[1], subjectName = row[2], path = row[3], url = row[4]))
        return res

    def getDatasets(self, skip, limit, onlyPublic, searchString):
        whereClause = sql.SQL("AND dataset.public = true" if onlyPublic else "")
        if searchString != '': 
            whereClause += sql.SQL(" AND dataset.name ILIKE {}").format(sql.Literal('%'+searchString+'%'))

        self.cursor.execute(sql.SQL("""
            SELECT dataset.id, dataset.name, author.name, dataset.creation_date, 
                   dataset.studies_count, dataset.patients_count
            FROM dataset, author 
            WHERE dataset.author_id = author.id AND dataset.invalidated = false {}
            LIMIT {} OFFSET {};""").format(whereClause, sql.SQL(str(limit)), sql.SQL(str(skip)))
        )
        res = []
        for row in self.cursor:
            res.append(dict(id = row[0], name = row[1], authorName = row[2], creationDate = str(row[3]), 
                            studiesCount = row[4], patientsCount = row[5]))
        return res
        
    def invalidateDataset(self, id):
        # logging.root.debug(self.cursor.mogrify("UPDATE dataset SET invalidated = true WHERE id = %s;", (id,)))
        self.cursor.execute("UPDATE dataset SET invalidated = true WHERE id = %s;", (id,))
