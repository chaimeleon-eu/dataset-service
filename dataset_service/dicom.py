
AGE_TAG = 0x0010, 0x1010
# Value format: DICOM VR AS (Age String) (https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html)
# Value examples: "073Y", "015M"

SEX_TAG = 0x0010, 0x0040
# Posible values: "M"-Male, "F"-Female, "O"-Other (https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part03/sect_C.2.3.html)

BODY_PART_TAG = 0x0018, 0x0015
# Values: https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part16/chapter_L.html

MODALITY_TAG = 0x0008, 0x0060
# Values: https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part16/sect_CID_29.html

DATASET_TYPE_TAG = 0x0008, 0x0016
# Values: https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_B.5.html

def getAgeInDays(dicomAge):
    age = int(dicomAge[:3])
    unit = dicomAge[-1:]
    if unit == "Y": return age*365
    if unit == "M": return age*30
    if unit == "W": return age*7
    if unit == "D": return age
    return None

def getAgeInMiabisFormat(dicomAge):
    age = int(dicomAge[:3])
    unit = dicomAge[-1:]
    if unit == "Y": unit = "years"
    if unit == "M": unit = "months"
    if unit == "W": unit = "weeks"
    if unit == "D": unit = "days"
    return age, unit

def getSexInMiabisFormat(dicomSex):
    if dicomSex == "M": return "Male"
    if dicomSex == "F": return "Female"
    return "Undifferentiated"   # "O"-Other