from datetime import datetime

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

def getAge(dicomAge) -> tuple[int, str]:
    age = int(dicomAge[:3])
    unit = dicomAge[-1:]
    if unit == "Y": return age*365, unit
    if unit == "M": return int(age*30.5), unit
    if unit == "W": return age*7, unit
    if unit == "D": return age, unit
    raise Exception("Dicom age cannot be parsed.")

def getDatetime(dicomDate: str) -> datetime:
    year = int(dicomDate[:4])
    month = int(dicomDate[4:6])
    day = int(dicomDate[6:8])
    return datetime(year, month, day)
