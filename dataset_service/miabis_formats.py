
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
    if sex is None: return None
    if sex == "M": return "Male"
    if sex == "F": return "Female"
    if sex == "O": return "Undifferentiated"
    raise Exception("Unexpected value")

