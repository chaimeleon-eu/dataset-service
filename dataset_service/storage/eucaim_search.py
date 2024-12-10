from psycopg2 import sql
import logging
import json
from .DB import DB
from . import eucaim_formats

class SearchValidationException(Exception): pass

def _ensureIsStr(value, conditionType, conditionKey):
    if not isinstance(value, str): 
        raise SearchValidationException(
            "'value' for type '%s' in condition for %s must be a string" % (conditionType, conditionKey))

def _ensureIsNum(value, conditionType, conditionKey):
    if not isinstance(value, (int, float)): 
        raise SearchValidationException(
            "'value' for type '%s' in condition for %s must be a number" % (conditionType, conditionKey))

def _ensureIsRangeOfNum(value, conditionType, conditionKey):
    if not isinstance(value, dict): 
        raise SearchValidationException("value for type '%s' in condition for %s must be an object" % (conditionType, conditionKey))
    if not 'min' in value: raise SearchValidationException("missing 'min' in range")
    if not 'max' in value: raise SearchValidationException("missing 'max' in range")
    if not isinstance(value["min"], (int, float)) or not isinstance(value["max"], (int, float)): 
            raise SearchValidationException("values for range condition for %s must be numbers" % conditionKey)

def _ensureIsArrayOfString(value, conditionType, conditionKey):
    if not isinstance(value, list): 
        raise SearchValidationException("value for type '%s' in condition for %s must be an array of strings" % (conditionType, conditionKey))
    for s in value:
        if not isinstance(s, str): 
            raise SearchValidationException("value for type '%s' in condition for %s must be an array of strings" % (conditionType, conditionKey))


def _searchConditionStringValueToSQL(key, type, value) -> sql.Composed:
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
        raise SearchValidationException("unknown 'type' in condition for %s" % key)


def _searchConditionNumValueToSQL(key, type, value) -> sql.Composed:
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
        raise SearchValidationException("unknown 'type' in condition for %s" % key)


def _searchConditionStringToSQL(type, value, key, db_column, translate) -> sql.Composed:
    if type in ["IN"]:
        _ensureIsArrayOfString(value, type, key)
        try:
            value = [translate(s) for s in value]
        except Exception as e: raise SearchValidationException("unknown 'value' in condition for %s" % key)
    elif type in ["EQUALS","NOT_EQUALS","CONTAINS"]:
        _ensureIsStr(value, type, key)
        try:
            value = translate(value)
        except Exception as e: raise SearchValidationException("unknown 'value' in condition for %s" % key)
    else: raise SearchValidationException("unknown 'type' in condition for %s" % key)
    return _searchConditionStringValueToSQL(db_column, type, value)


def _searchConditionNumToSQL(type, value, key, db_column, translate) -> sql.Composed:
    if type in ["BETWEEN"]:
        _ensureIsRangeOfNum(value, type, key)
        value["min"] = translate(value["min"])
        value["max"] = translate(value["max"])
    elif type in ["LOWER_THAN","GREATER_THAN"]:
        if isinstance(value, dict): 
            # strange case but accepted if value is a range, just one of the limits will be taken
            _ensureIsRangeOfNum(value, type, key)
            if type == "LOWER_THAN": 
                value = value["max"]  # ignore min
                type = "LOWER_EQUAL_THAN"
            else:  # type == "GREATER_THAN"
                value = value["min"]  # ignore max
                type = "GREATER_EQUAL_THAN"
        else:
            _ensureIsNum(value, type, key)
        value = translate(value)
    elif type in ["EQUALS","NOT_EQUALS"]:
        _ensureIsNum(value, type, key)
        value = translate(value)
    else: raise SearchValidationException("unknown 'type' in condition for %s" % key)
    return _searchConditionNumValueToSQL(db_column, type, value)

@staticmethod
def _sqlSeriesConditionsToSqlStudiesCondition(sqlSeriesCondition: sql.Composable) -> sql.Composable:
    return sql.SQL("""EXISTS (
                        SELECT series.folder_name FROM dataset_study_series, series 
                        WHERE dataset_study_series.dataset_id = dataset_study.dataset_id
                            AND dataset_study_series.study_id = dataset_study.study_id
                            AND series.study_id = dataset_study_series.study_id
                            AND series.folder_name = dataset_study_series.series_folder_name
                            AND {}
                        )""").format(sqlSeriesCondition)

@staticmethod
def _searchRequestToSQL(sr) -> tuple[sql.Composable, bool]:
    if 'operand' in sr:   # it is an OPERATION: AND/OR of CONDITIONs
        if not sr['operand'] in ['AND', 'OR']: raise SearchValidationException("unknown value for 'operand'")
        if not 'children' in sr:                raise SearchValidationException("missing 'children' in operation")
        if not isinstance(sr['children'], list): raise SearchValidationException("'children' in operation must be an array")
        if len(sr['children']) == 0: return sql.SQL(""), False
        sqlStudiesConditions = []
        sqlSeriesConditions = []
        for child in sr['children']:
            sqlCondition, isSeriesCondition = _searchRequestToSQL(child)
            if isSeriesCondition: sqlSeriesConditions.append(sqlCondition)
            else:                 sqlStudiesConditions.append(sqlCondition)
        if len(sqlStudiesConditions) > 0:
            if len(sqlSeriesConditions) > 0:  # both studies and series conditions
                seriesOperation = sql.SQL(' %s ' % sr['operand']).join(sqlSeriesConditions)
                seriesOperation = sql.SQL("(")+seriesOperation+sql.SQL(")")
                seriesOperation = _sqlSeriesConditionsToSqlStudiesCondition(seriesOperation)
                sqlStudiesConditions.append(seriesOperation)
            operation = sql.SQL(' %s ' % sr['operand']).join(sqlStudiesConditions)
            return sql.SQL("(")+operation+sql.SQL(")"), False
        else: # only series conditions
            operation = sql.SQL(' %s ' % sr['operand']).join(sqlSeriesConditions)
            return sql.SQL("(")+operation+sql.SQL(")"), True
    elif 'key' in sr:   # it is a CONDITION
        if not 'type' in sr: raise SearchValidationException("missing 'type' in condition")
        if not 'value' in sr: raise SearchValidationException("missing 'value' in condition")
        if sr['key'] == 'SNOMEDCT263495000':  # gender
            res = _searchConditionStringToSQL(sr['type'], sr['value'], 'gender', 'study.sex', eucaim_formats.getGender)
        elif sr['key'] == 'SNOMEDCT423493009':  # age at diagnosis
            res = _searchConditionNumToSQL(sr['type'], sr['value'], 'age', 'study.age_in_days', eucaim_formats.getAge)
        elif sr['key'] == 'SNOMEDCT439401001':  # diagnosis
            res = _searchConditionStringToSQL(sr['type'], sr['value'], 'diagnosis', 'study.diagnosis', eucaim_formats.getDiagnosis)
        elif sr['key'] == 'SNOMEDCT432213005':  # year_of_diagnosis
            res = _searchConditionNumToSQL(sr['type'], sr['value'], 'year of diagnosis', 'study.diagnosis_year', eucaim_formats.getYear)
        elif sr['key'] == 'RID10311':  # modality   SNOMEDCT363679005
            res = _searchConditionStringToSQL(sr['type'], sr['value'], 'modality', 'series.modality', eucaim_formats.getModality)
        elif sr['key'] == 'SNOMEDCT123037004':  # body part   # mejor SNOMEDCT38866009 ?
            res = _searchConditionStringToSQL(sr['type'], sr['value'], 'body part', 'series.body_part', eucaim_formats.getBodyPart)
        elif sr['key'] == 'C25392':  # manufacturer
            res = _searchConditionStringToSQL(sr['type'], sr['value'], 'Manufacturer', 'series.manufacturer', eucaim_formats.getManufacturer)
        else: raise SearchValidationException("unkown 'key' in condition")
        # Modality, body part and manufacturer are properties of series
        isSeriesCondition = (sr['key'] in ['RID10311', 'SNOMEDCT123037004', 'C25392'])
        return sql.SQL("(") + res + sql.SQL(")"), isSeriesCondition
    else: raise SearchValidationException("missing 'operand' or 'key'")

class DBDatasetsEUCAIMSearcher():
    def __init__(self, db: DB):
        self.cursor = db.cursor
        self.conn = db.conn

    def eucaimSearchDatasets(self, skip, limit, searchRequest):
        whereClause, isSeriesCondition = _searchRequestToSQL(searchRequest)
        if isSeriesCondition:
            whereClause = _sqlSeriesConditionsToSqlStudiesCondition(whereClause)
        if whereClause != sql.SQL(""):
            whereClause = sql.SQL("AND ") + whereClause
        if limit == 0: limit = 'ALL'
        q = sql.SQL("""
                SELECT dataset.id, dataset.name, dataset.creation_date, 
                    dataset.draft, dataset.public, dataset.invalidated, 
                    COUNT(study.id), COUNT(DISTINCT study.subject_name), 
                    dataset.age_low_in_days, dataset.age_high_in_days, dataset.sex, 
                    dataset.modality, dataset.body_part, dataset.description
                FROM dataset, dataset_study, study
                WHERE dataset.id = dataset_study.dataset_id AND dataset_study.study_id = study.id
                      AND dataset.public = true AND dataset.draft = false AND dataset.invalidated = false {}
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