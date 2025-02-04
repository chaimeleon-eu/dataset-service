from datetime import datetime
import pydicom
import logging

# List of tags with types (VR):
#   https://dicom.nema.org/medical/dicom/current/output/chtml/part06/chapter_6.html
# List of types (VR), allowed chars and max length:
#   https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html

AGE_TAG = 0x0010, 0x1010
# Value format: DICOM VR AS (Age String) (https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html)
# Value examples: "073Y", "015M"

SEX_TAG = 0x0010, 0x0040
# Posible values: "M"-Male, "F"-Female, "O"-Other (https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part03/sect_C.2.3.html)

BODY_PART_TAG = 0x0018, 0x0015
# Values: https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part16/chapter_L.html

MODALITY_TAG = 0x0008, 0x0060
# Values: https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part16/sect_CID_29.html
# https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part03/sect_C.7.3.html#sect_C.7.3.1.1.1

MANUFACTURER_TAG = 0x0008, 0x0070
# Values: https://dicom.nema.org/medical/Dicom/2016e/output/chtml/part03/sect_C.7.5.html

STUDY_DATE_TAG = 0X0008, 0X0020
# Value format: DICOM VR DA (Date) (https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html)
# Value example: "19930822" would represent August 22, 1993.

DATASET_TYPE_TAG = 0x0008, 0x0016
# Values: https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_B.5.html

PROJECT_NAME_PRIVATE_TAG = 0x70D1, 0x2000
# Value examples: "Colon cancer CT_only", "Lung cancer CT_only"

class Dicom:
    def __init__(self, dicomFilePath):
        self.dcm = pydicom.dcmread(dicomFilePath, stop_before_pixels=True, force=True)
    
    def getFileName(self) -> str:
        return str(self.dcm.filename)

    def getAge(self) -> tuple[int|None, str|None]:
        if not AGE_TAG in self.dcm: return None, None
        value = self.dcm[AGE_TAG].value
        try:
            age = int(value[:3])
            unit = value[-1:]
            if unit == "Y": return age*365, unit
            if unit == "M": return int(age*30.5), unit
            if unit == "W": return age*7, unit
            if unit == "D": return age, unit
        except Exception: pass
        logging.root.warning("Invalid value '%s' for dicom tag patientAge, cannot be parsed (%s)." \
                             % (value, self.dcm.filename))
        return None, None

    def getStudyDate(self) -> datetime|None:
        if not STUDY_DATE_TAG in self.dcm: return None
        value = self.dcm[STUDY_DATE_TAG].value
        year = int(value[:4])
        month = int(value[4:6])
        day = int(value[6:8])
        return datetime(year, month, day)

    def getBodyPart(self) -> str | None:
        if not BODY_PART_TAG in self.dcm: return None
        value = self.dcm[BODY_PART_TAG].value
        if value == "": return None
        return value

    def getSex(self) -> str | None:
        if not SEX_TAG in self.dcm: return None
        value = self.dcm[SEX_TAG].value
        if value == "": return None
        return value

    def getModality(self) -> str | None:
        if not MODALITY_TAG in self.dcm: return None
        value = self.dcm[MODALITY_TAG].value
        if value == "": return None
        return value
    
    def getDatasetType(self) -> str | None:
        if not DATASET_TYPE_TAG in self.dcm: return None
        value = self.dcm[DATASET_TYPE_TAG].value
        if value == "": return None
        return value

    def getManufacturer(self) -> str | None:
        if not MANUFACTURER_TAG in self.dcm:  return None
        value = self.dcm[MANUFACTURER_TAG].value
        # Dicom don't specifies the possible values,
        # so, in order to harmonize, we take all the possible values from eucaim ontology,
        # plus the 'Other' value for new manufacturers not included there.
        if value == "": return None
        value = value.lower()
        if value.find('adac') >= 0: return 'ADAC'
        elif value.find('agfa') >= 0: return 'Agfa'
        elif value.find('canon') >= 0: return 'Canon'
        elif value.find('elscint') >= 0: return 'Elscint'
        elif value.find('esaote') >= 0: return 'Esaote'
        elif value.find('fujifilm') >= 0: return 'Fujifilm'
        elif value.find('general electric') >= 0 \
            or value.find('ge ') >= 0: return 'General Electric'
        elif value.find('hitachi') >= 0: return 'Hitachi'
        elif value.find('hologic') >= 0: return 'Hologic'
        elif value.find('i.m.s') >= 0 \
            or value.find('ims ') >= 0 \
            or value == 'ims': return 'I.M.S'
        elif value.find('marconi') >= 0: return 'Marconi'
        elif value.find('mediso') >= 0: return 'Mediso'
        elif value.find('mie') >= 0: return 'MiE'
        elif value.find('philips') >= 0: return 'Philips'
        elif value.find('picker') >= 0: return 'Picker International'
        elif value.find('shimadzu') >= 0: return 'Shimadzu'
        elif value.find('siemens') >= 0: return 'Siemens'
        elif value.find('toshiba') >= 0: return 'Toshiba'
        else: return 'Other'

    def getDiagnosis(self) -> str | None:
        if not PROJECT_NAME_PRIVATE_TAG in self.dcm: return None
        project_name = self.dcm[PROJECT_NAME_PRIVATE_TAG].value
        diagnosis = ' '.join(str(project_name).split(' ')[:2])
        diagnosis.lower().capitalize()
        if diagnosis in ["Lung cancer", "Colon cancer", "Rectum cancer", "Breast cancer", "Prostate cancer"]: 
            return diagnosis
        else:
            return None
            

