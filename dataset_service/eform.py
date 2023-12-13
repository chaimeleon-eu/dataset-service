import json
from datetime import datetime

class Eform:
    diagnosisYear = None
    ageInDays = None
    ageUnit = None
    sex = None

    def __init__(self, eform):
        if not "pages" in eform or not isinstance(eform["pages"], list): return None
        for page in eform["pages"]:
            if not "page_name" in page or not "page_data" in page: continue
            if page["page_name"] == "inclusion_criteria":
                try:
                    if "baseline_date" in page["page_data"]:  # prostate, rectum, colon
                        value = page["page_data"]["baseline_date"]["value"]
                        self.diagnosisYear = datetime.fromisoformat(value).year
                    elif "date_baseline_ct" in page["page_data"]:  # lung
                        value = page["page_data"]["date_baseline_ct"]["value"]
                        self.diagnosisYear = datetime.fromisoformat(value).year
                except: pass
                try:
                    if "age_at_diagnosis" in page["page_data"]:  # prostate, rectum, colon
                        value = page["page_data"]["age_at_diagnosis"]["value"]
                        self.ageInDays = value * 365
                        self.ageUnit = "Y"
                    elif "age_at_baseline" in page["page_data"]:  # lung
                        value = page["page_data"]["age_at_baseline"]["value"]
                        self.ageInDays = value * 365
                        self.ageUnit = "Y"
                except: pass
            if page["page_name"] == "patient_data":
                if "gender" in page["page_data"]:
                    try:
                        value = page["page_data"]["gender"]["value"]
                        if value == "MALE": sex  = "M"
                        elif value == "FEMALE": sex  = "F"
                        else: sex = "O"
                        self.sex = sex
                    except: pass

class Eforms:
    def __init__(self, eformsFilePath):
        with open(eformsFilePath, 'rb') as f:
            contentBytes = f.read()
        subjectsList = json.loads(contentBytes)
        self.subjects = dict([ (subject["subjectName"], subject["eForm"]) for subject in subjectsList ])

    def getEform(self, subjectName) -> Eform:
        return Eform(self.subjects[subjectName])

