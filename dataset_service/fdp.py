from sempyro.dcat import DCATDataset
import pandas as pd
import pprint
from rdflib import URIRef
import logging
from .dataset_ext import DCATResourceNxt, SKOSConcept




# def toDCAT(dataset):
def toDCAT(dataset):# Transformaciones de los campos a sus correspondientes datatypes
   
   LOG = logging.root
   serialized_graphs = []
   LOG.info(f"VISUALIZACIÓN dataset: {dataset}")

   df = pd.DataFrame([dataset])
# ->> 'access_rights': debe ser un valor de tipo 'AccessRights' en formato URI ((NO TENEMOS URI SIMILAR EN EL DATASET)) EL CAMPO PUBLIC ES BOOLEANO
#  df["access_rights"] = df["public"].apply(lambda x: "http://purl.org/accessrights/Public" if x else "http://purl.org/accessrights/Restricted")

# 'conforms_to': debe ser un valor de tipo URL (para la licencia) ((LICENSE SÍ TIENE UNA URL DE LICENCIA))
   # Define the mapping for access rights based on public status
   def map_access_rights(public_flag):
      if public_flag:
         return URIRef("http://publications.europa.eu/resource/authority/access-right/PUBLIC")
      else:
         return URIRef("http://publications.europa.eu/resource/authority/access-right/RESTRICTED")


   df["title"] = df["name"].apply(lambda x: [x])

   df["description"] = df["description"].apply(lambda x: [x])

   df["contact_point"] = df["contactInfo"].apply(parse_contact_info)

   df["publisher"] = df["project"].apply(lambda x: [x])

##Asegurarnos de que es este realmente el formato correcto 
   df["theme"] = "http://eurovoc.europa.eu/c_efec98c3"
   # "http://publications.europa.eu/resource/authority/data-theme/HEAL"
   df["identifier"] = df["id"].apply( lambda x: [x])

   # Apply the access_rights mapping
   df["access_rights"] = df["public"].apply(map_access_rights)
   #Asegurarnos de que es igual qu eel anterior
   df["rights"] = df["public"].apply(map_access_rights)
   #EUCAIM controlled vocabulary: "Authorisation to download the datasets
# Authorisation to access, view and process in-situ the datasets 
# Authorisation to remotely process the datasets without the ability to access and visualise data, even remotely."

   df["applicableLegislation"] = "http://data.europa.eu/eli/reg/2022/868/oj"

## Provenance ahora devuelve un string desde chaimeleon simplemente sacarlo de la api con el mismo nombre
##   df["provenance"] = ""

## Saber si es válido /HEALTH y ver campo añadido por pau. y ver que tipo 
   df["type"] = "http://publications.europa.eu/resource/authority/dataset-type/"

   df["version"] = df["version"].apply(lambda x: [x])

   df["ageLow"]  = df["ageLow"].apply(lambda x: [x])

   df["ageHigh"] = df["ageHigh"].apply(lambda x: [x])

   df["nbrOfStudies"] = df["studiesCount"].apply(lambda x: [x])

   df["nbrOfSubjects"] = df["subjectsCount"].apply(lambda x: [x])

## Revisar LegalBasis
   df["legalBasis"] = "http://data.europa.eu/eli/reg/2022/868/oj"

   df["intendedPurpose"] = df["purpose"].apply(lambda x: [x])


## definir el  quality annotation y comprender de donde obtenerlo
##   df["qualityLabel"] = 

   df["collectionMethod"] = df["collectionMethod"].apply(lambda x: [x])

   # df["conforms_to"] = df["license"].apply(lambda x: x["url"])

   df["sex"] = df["sex"].apply(lambda x: [x])

## REvisar de dónde obtener la condition.//En teoría va en diagnosis. Mensaje pau.
##  df["condition"] = "http://purl.bioontology.org/ontology/SNOMEDCT/399068003"
#  Añadir terminos de snomed, si diagnosis devuelve un diagnostico tipo lung devulver la url entera para lung como la del ejemplo, qu een este caso es de prostata

   # 'contact_point': debe ser una lista de tipo 'Url' o 'VCard' (uso de VCard para nombre y email)
##   df["hasImageModality"] = df["modality"].apply(lambda x: [SKOSConcept(prefLabel=x)])
#transformar de ct pt a modalidad radlex
# Tiene poco que ver con la url de enola eucaim:hasImageModality <http://www.radlex.org/RID/RID10312
   
   df["hasImageBodyPart"] = df["bodyPart"].apply(lambda x: [SKOSConcept(prefLabel=x)])
   
   def parse_contact_info(contact_info):
      # Registrar el contenido de 'contactInfo' para debug usando el logger existente
      if not contact_info:
         LOG.warning(f"contactInfo está vacío o es None: {contact_info}")
      else:
         LOG.info(f"Procesando contactInfo: {contact_info}")

      try:
         # Verificar que contact_info no sea None y esté en el formato esperado
         if contact_info and len(contact_info.split()) >= 2:
               name, email = contact_info.split(" ")[0], contact_info.split(" ")[1][1:-1]
               return [{"vcard:fn": name, "vcard:email": email}]
         else:
               LOG.warning(f"Formato incorrecto en contactInfo: {contact_info}")
               return []  # Retorna lista vacía si el formato es incorrecto o no hay suficientes datos
      except Exception as e:
         LOG.error(f"Error procesando contactInfo: {contact_info}, Error: {e}")
         return []  # Retorna lista vacía si hay algún otro error inesperado



   # Eliminación de columnas innecesarias y renombrado de columnas
   df.drop(columns=[
      "authorName", "authorId", "public", "pids", "previousId", "nextId", "contactInfo", "license",
      "id", "name", "project", "creationDate", "purpose", "collectionMethod", "draft", 
      "invalidated", "corrupted", "lastIntegrityCheck", "studiesCount", "subjectsCount", "ageLow", 
      "ageHigh", "ageUnit", "ageNullCount", "sex", "sexCount", "diagnosisYearLow", "diagnosisYearHigh", 
      "diagnosisYearNullCount", "bodyPart", "bodyPartCount", "modality", "modalityCount", 
      "manufacturer", "manufacturerCount", "seriesTags", "sizeInBytes", "description", "version", "type"
   ], inplace=True)
   
   datasets = df.to_dict('records')   
   dcat_datasets = [DCATResourceNxt(**x) for x in datasets]   
   for item in dcat_datasets:
      serialized_graph = item.to_graph(URIRef(item.identifier[0])).serialize()
      print(serialized_graph)
      serialized_graphs.append(serialized_graph)
   
   
   dcat_fields = DCATResourceNxt.annotate_model()
   pprint.pprint(dcat_fields.fields_description())
   return serialized_graphs