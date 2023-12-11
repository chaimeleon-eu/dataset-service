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

def getBodyPart(dicomBodyPart: str) -> str | None:
    if dicomBodyPart == "": return None
    return dicomBodyPart

def getSex(dicomSex: str) -> str | None:
    if dicomSex == "": return None
    return dicomSex

def getModality(dicomModality: str) -> str | None:
    if dicomModality == "": return None
    return dicomModality

def getManufacturer(dicomManufacturer: str) -> str | None:
    # Dicom don't specifies the possible values,
    # so, in order to harmonize, we take all the possible values from eucaim ontology,
    # plus the 'Other' value for new manufacturers not included there.
    if dicomManufacturer == "": return None
    dicomManufacturer = dicomManufacturer.lower()
    if dicomManufacturer.find('adac') >= 0: return 'ADAC'
    elif dicomManufacturer.find('agfa') >= 0: return 'Agfa'
    elif dicomManufacturer.find('canon') >= 0: return 'Canon'
    elif dicomManufacturer.find('elscint') >= 0: return 'Elscint'
    elif dicomManufacturer.find('esaote') >= 0: return 'Esaote'
    elif dicomManufacturer.find('fujifilm') >= 0: return 'Fujifilm'
    elif dicomManufacturer.find('general electric') >= 0 \
        or dicomManufacturer.find('ge ') >= 0: return 'General Electric'
    elif dicomManufacturer.find('hitachi') >= 0: return 'Hitachi'
    elif dicomManufacturer.find('hologic') >= 0: return 'Hologic'
    elif dicomManufacturer.find('i.m.s') >= 0 \
        or dicomManufacturer.find('ims ') >= 0 \
        or dicomManufacturer == 'ims': return 'I.M.S'
    elif dicomManufacturer.find('marconi') >= 0: return 'Marconi'
    elif dicomManufacturer.find('mediso') >= 0: return 'Mediso'
    elif dicomManufacturer.find('mie') >= 0: return 'MiE'
    elif dicomManufacturer.find('philips') >= 0: return 'Philips'
    elif dicomManufacturer.find('picker') >= 0: return 'Picker International'
    elif dicomManufacturer.find('shimadzu') >= 0: return 'Shimadzu'
    elif dicomManufacturer.find('siemens') >= 0: return 'Siemens'
    elif dicomManufacturer.find('toshiba') >= 0: return 'Toshiba'
    else: return 'Other'
