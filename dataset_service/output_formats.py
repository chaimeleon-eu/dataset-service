
# MIABIS: https://github.com/BBMRI-ERIC/miabis/blob/master/Data-describing-Sample-Collection.md
# DICOM extension: https://eurradiolexp.springeropen.com/articles/10.1186/s41747-021-00214-4

def ageToMiabis(ageInDays, unit) -> tuple[int, str]:
    if unit == "Y": return round(ageInDays/365), "years"
    if unit == "M": return round(ageInDays/30.5), "months"
    if unit == "W": return round(ageInDays/7), "weeks"
    # if unit == "D": 
    return ageInDays, "days"
    
def ageDicomToMiabis(dicomAge):
    age = int(dicomAge[:3])
    unit = dicomAge[-1:]
    if unit == "Y": unit = "years"
    if unit == "M": unit = "months"
    if unit == "W": unit = "weeks"
    if unit == "D": unit = "days"
    return age, unit

def sexToMiabis(sex):
    if sex is None: return "Unkown"
    if sex == "M": return "Male"
    if sex == "F": return "Female"
    if sex == "O": return "Undifferentiated"
    raise Exception("Unexpected value")

def bodyPartToOutputFormat(bodyPart):
    # Body part is not in Miabis, we take the dicom values,
    # but we use 'Unknown' to represent the empty value instead of '""' used by dicom.
    if bodyPart is None: return 'Unknown'
    else: return bodyPart

def modalityToOutputFormat(modality):
    # Modality is not in Miabis, we take the dicom values,
    # but we use 'Unknown' to represent the empty value instead of '""' used by dicom.
    if modality is None: return 'Unknown'
    else: return modality

def manufacturerToOutputFormat(manufacturer):
    # Manufacturer is not in Miabis and dicom don't specifies the possible values,
    # we take the possible values from eucaim ontology 
    # plus the 'Other' value for new manufacturers not included there,
    # and finally we add the value 'Unknown' to represent the empty value.
    if manufacturer is None: return 'Unknown'
    else: return manufacturer
