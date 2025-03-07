from psycopg2 import sql
from .DB import DB

class DBDatasetAccessesOperator():
    def __init__(self, db: DB):
        self.cursor = db.cursor

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
            self.updateDatasetTimesUsed(id)
    
    def updateDatasetTimesUsed(self, id):
        self.cursor.execute("""
            UPDATE dataset
            SET times_used = 
                (SELECT COUNT(*) FROM dataset_access_dataset WHERE dataset_id = %s) 
            WHERE id = %s;""", 
            (id, id))

    def existsDatasetAccess(self, datasetAccessId):
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

    def getDatasetAccesses(self, datasetId, limit = 0, skip = 0):
        if limit == 0: limit = 'ALL'

        # First get total rows without LIMIT and OFFSET
        self.cursor.execute("""SELECT count(*) FROM dataset_access, dataset_access_dataset 
                               WHERE dataset_access_dataset.dataset_id = %s
                               AND dataset_access_dataset.dataset_access_id = dataset_access.id """, (datasetId,))
        row = self.cursor.fetchone()
        total = row[0] if row != None else 0

        self.cursor.execute(sql.SQL("""
            SELECT dataset_access.creation_time, author.username, dataset_access.access_type, 
                   dataset_access.tool_name, dataset_access.tool_version, dataset_access.image, 
                   dataset_access.resource_flavor, 
                   dataset_access.start_time, dataset_access.end_time, dataset_access.end_status, 
                   dataset_access.cmd_line, dataset_access.openchallenge_job_type
            FROM dataset_access, dataset_access_dataset, author
            WHERE dataset_access_dataset.dataset_id = %s
                  AND dataset_access_dataset.dataset_access_id = dataset_access.id 
                  AND dataset_access.user_gid = author.gid
            ORDER BY dataset_access.creation_time DESC
            LIMIT {} OFFSET {};""").format(sql.SQL(str(limit)), sql.SQL(str(skip))), 
            (datasetId,))
        res = []
        for row in self.cursor:
            startTime, endTime, duration = row[7], row[8], None
            if startTime != None and endTime != None:
                duration = (endTime - startTime).total_seconds()/60
            creationTime = str(row[0].astimezone())   # row[0] is a datetime without time zone, just add the local tz.
                                                      # If local tz is UTC, the string "+00:00" is added at the end.
            startTime = str(startTime.astimezone()) if startTime != None else None
            endTime = str(endTime.astimezone()) if endTime != None else None
            res.append(dict(creationTime = creationTime, username = row[1], accessType = row[2], 
                            toolName = row[3], toolVersion = row[4], image = row[5],
                            resourcesFlavor = row[6], duration = duration,
                            startTime = startTime, endTime = endTime, endStatus = row[9],
                            cmdLine = row[10], openchallengeJobType = row[11]))
        return res, total

    def deleteDatasetAccess(self, datasetAccessId):
        self.cursor.execute("DELETE FROM dataset_access_dataset WHERE dataset_access_id=%s;", (datasetAccessId,))
        self.cursor.execute("DELETE FROM dataset_access WHERE id=%s;", (datasetAccessId,))
        
    def endDatasetAccess(self, datasetAccessId, startTime, endTime, endStatus):
        self.cursor.execute("""
            UPDATE dataset_access set start_time=%s, end_time=%s, end_status=%s, closed=TRUE
            WHERE id=%s;""",
            (startTime, endTime, endStatus, datasetAccessId))

