from datetime import datetime

# snomed codes: 
#  https://browser.ihtsdotools.org/?perspective=full&conceptId1=38866009&edition=MAIN/2023-11-01&release=&languages=en

def getGender(eucaimGender):
    if eucaimGender == "SNOMEDCT261665006": return None  # unknown
    if eucaimGender == "SNOMEDCT248153007": return "M"  # Male
    if eucaimGender == "SNOMEDCT248152002": return "F"  # Female
    if eucaimGender == "SNOMEDCT74964007": return "O"  # Other
    raise Exception()

def genderToEucaim(gender):
    if gender is None: return "SNOMEDCT261665006"  # unknown
    if gender == "M": return "SNOMEDCT248153007"  # Male
    if gender == "F": return "SNOMEDCT248152002"  # Female
    return "SNOMEDCT74964007"   # "O"-Other

def getDiagnosis(eucaimDiagnosis):
    if eucaimDiagnosis == "SNOMEDCT261665006": return None
    if eucaimDiagnosis == "SNOMEDCT399068003": return "Prostate cancer"
    if eucaimDiagnosis == "SNOMEDCT254837009": return "Breast cancer"
    if eucaimDiagnosis == "SNOMEDCT363358000": return "Lung cancer"
    if eucaimDiagnosis == "SNOMEDCT363406005": return "Colon cancer"
    if eucaimDiagnosis == "SNOMEDCT363351006": return "Rectum cancer"
    raise Exception()

def getBodyPart(eucaimBodyPart):
    # https://dicom.nema.org/medical/dicom/current/output/chtml/part16/chapter_L.html#table_L-1
    if eucaimBodyPart == "SNOMEDCT261665006": return None
    if eucaimBodyPart == "SNOMEDCT41216001": return "PROSTATE"
    if eucaimBodyPart == "SNOMEDCT76752008": return "BREAST"
    if eucaimBodyPart == "SNOMEDCT39607008": return "LUNG"
    if eucaimBodyPart == "SNOMEDCT71854001": return "COLON"
    if eucaimBodyPart == "SNOMEDCT818981001": return "ABDOMEN"
    if eucaimBodyPart == "SNOMEDCT816092008": return "PELVIS"
    if eucaimBodyPart == "SNOMEDCT69536005": return "HEAD"
    raise Exception()

def getModality(eucaimModality):
    if eucaimModality == "SNOMEDCT261665006": return None
    if eucaimModality == "RID10312": return "MR"   # MRI    SNOMEDCT113091000 
    if eucaimModality == "RID10337": return "PT"   # PET
    if eucaimModality == "RID10321": return "CT"
    if eucaimModality == "RID10334":   # SPECT 
    #    return "ST"
    # It was retired and incorporated in the modality "NM"
    # https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part03/sect_C.7.3.html#sect_C.7.3.1.1.1  (Retired Defined Terms, Note 6)
        return "NM"
    raise Exception()

def getAge(eucaimAge):
    return eucaimAge*365

def getDateTime(eucaimYear):
    year = int(eucaimYear)
    month = 1
    day = 1
    return datetime(year, month, day)

def getYear(eucaimYear):
    return eucaimYear

def getManufacturer(eucaimManufacturer):
    if eucaimManufacturer == "SNOMEDCT261665006": return None
    if eucaimManufacturer == "C200140": return "Siemens"       # Siemens Healthineers
    if eucaimManufacturer == "birnlex_3066": return "Siemens"   # Siemens Medical Solutions
    if eucaimManufacturer == "birnlex_12833": return "General Electric"  # General Electric Medical Systems # Other values seen: "GE HEALTHCARE"
    if eucaimManufacturer == "birnlex_3065": return "Philips" # Philips Medical Systems # Other values seen: "Philips Medical Systems"
    if eucaimManufacturer == "birnlex_3067": return "Toshiba"  # Toshiba Medical Solutions
                                                            # ESAOTE
                                                            #IMS # Other values seen: "IMS s.r.l.", "IMS GIOTTO S.p.A."
    raise Exception()

