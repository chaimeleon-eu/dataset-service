from sempyro.dcat import DCATDataset
import pprint
from sempyro import LiteralField, RDFModel
from rdflib import URIRef
import logging
from .dataset_ext import DCATResourceNxt, SKOSConcept, ProvenanceStatementModel


def toDCAT(dataset): # Transformaciones de los campos a sus correspondientes datatypes
   LOG = logging.root
   LOG.info(f"Translating dataset: {dataset}")
   dcat_dataset = dict()

# ->> 'access_rights': debe ser un valor de tipo 'AccessRights' en formato URI ((NO TENEMOS URI SIMILAR EN EL DATASET)) EL CAMPO PUBLIC ES BOOLEANO
   # dcat_dataset["access_rights"] = "http://purl.org/accessrights/Public" if dataset["public"] else "http://purl.org/accessrights/Restricted"


# 'conforms_to': debe ser un valor de tipo URL (para la licencia) ((LICENSE SÍ TIENE UNA URL DE LICENCIA))
   # Define the mapping for access rights based on public status
   def map_access_rights(public_flag):
      if public_flag:
         return URIRef("http://publications.europa.eu/resource/authority/access-right/PUBLIC")
      else:
         return URIRef("http://publications.europa.eu/resource/authority/access-right/RESTRICTED")

   def map_rights(is_public):
      if is_public:
         return "The dataset is public and accessible to everyone."
      else:
         return "Access to the dataset is restricted and requires authorization."

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

   # def fill_title_from_name(cls, values):
   #    if 'name' in values:
   #       # Creamos el LiteralField con la string que ya existe en 'name'
   #       values['title'] = [LiteralField(value=values['name'])]
      return values
   
   dcat_dataset["title"] = [LiteralField(value=dataset["name"])]
   dcat_dataset["description"] = [dataset["description"]]
   dcat_dataset["contact_point"] = parse_contact_info(dataset["contactInfo"])

   #ESPERANDO SRESPUESTA VALIA
   # dcat_dataset["publisher"] = [dataset["project"]]

##Asegurarnos de que es este realmente el formato correcto
################## ENVIAR PREGUNTA A VALIA theme 
   # dcat_dataset["theme"] = "http://eurovoc.europa.eu/c_efec98c3"
   # "http://publications.europa.eu/resource/authority/data-theme/HEAL"
   dcat_dataset["identifier"] = [dataset["id"]]

   # Apply the access_rights mapping
   dcat_dataset["access_rights"] = map_access_rights(dataset["public"])
   #Asegurarnos de que es igual qu eel anterior
   dcat_dataset["rights"] = map_rights(dataset["public"])
   #EUCAIM controlled vocabulary: "Authorisation to download the datasets
# Authorisation to access, view and process in-situ the datasets 
# Authorisation to remotely process the datasets without the ability to access and visualise data, even remotely."

   dcat_dataset["applicableLegislation"] = "http://data.europa.eu/eli/reg/2022/868/oj"

## Provenance ahora devuelve un string desde chaimeleon simplemente sacarlo de la api con el mismo nombre
   # dcat_dataset["provenance"] = [dataset["provenance"]]

   def to_provenance_statements(text):
      """
      Transforma un string (o None) en una lista de ProvenanceStatementModel.
      """
      if not text:
         return None  # O [] si prefieres devolver una lista vacía
    # text es un string, creamos una instancia con label=text.
      return [ProvenanceStatementModel(label=text)]

   dcat_dataset["provenance"] = to_provenance_statements(dataset["provenance"])

## Saber si es válido /HEALTH y ver campo añadido por pau. y ver que tipo 
   dcat_dataset["type"] = "https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#Dataset"

   dcat_dataset["version"] = [dataset["version"]]
   dcat_dataset["ageLow"] = [dataset["ageLow"]]
   dcat_dataset["ageHigh"] = [dataset["ageHigh"]]
   dcat_dataset["nbrOfStudies"] = [dataset["studiesCount"]]
   dcat_dataset["nbrOfSubjects"] = [dataset["subjectsCount"]]

## Revisar LegalBasis
   dcat_dataset["legalBasis"] = "http://data.europa.eu/eli/reg/2022/868/oj"

   dcat_dataset["intendedPurpose"] = [dataset["purpose"]]

## definir el  quality annotation y comprender de donde obtenerlo
##   dcat_dataset["qualityLabel"] = 

   dcat_dataset["collectionMethod"] = [dataset["collectionMethod"]]

   # dcat_dataset["conforms_to"] = dataset["license"]["url"]

   dcat_dataset["sex"] = [dataset["sex"]]
   
   SNOMED_MAPPING = {
      "Prostate cancer": "http://purl.bioontology.org/ontology/SNOMEDCT/399068003",
      "Breast cancer": "http://purl.bioontology.org/ontology/SNOMEDCT/254837009",
      "Lung cancer": "http://purl.bioontology.org/ontology/SNOMEDCT/254637007",
      "Colon cancer": "http://purl.bioontology.org/ontology/SNOMEDCT/363406005",
      "Rectum cancer": "http://purl.bioontology.org/ontology/SNOMEDCT/363349007",
      "Unknown": None  
   }
   
   def map_condition(diagnosis_list):
      if not diagnosis_list:
         return [] 

      if isinstance(diagnosis_list, str):
         return [SKOSConcept(prefLabel=diagnosis_list, uri=SNOMED_MAPPING.get(diagnosis_list))]

      if isinstance(diagnosis_list, list):
         return [
               SKOSConcept(prefLabel=diag, uri=SNOMED_MAPPING.get(diag))
               for diag in diagnosis_list if diag in SNOMED_MAPPING
         ]

      raise ValueError(f"Unexpected value in diagnosis: {diagnosis_list}")

   dcat_dataset["condition"] = map_condition(dataset["diagnosis"])
#  Añadir terminos de snomed, si diagnosis devuelve un diagnostico tipo lung devulver la url entera para lung como la del ejemplo, qu een este caso es de prostata

   # 'contact_point': debe ser una lista de tipo 'Url' o 'VCard' (uso de VCard para nombre y email)
##   dcat_dataset["hasImageModality"] = [SKOSConcept(prefLabel=dataset["modality"])]
#transformar de ct pt a modalidad radlex
# Tiene poco que ver con la url de enola eucaim:hasImageModality <http://www.radlex.org/RID/RID10312
   
   def convert_body_parts_to_skos(body_parts):
      """
      Convierte el valor en la columna 'bodyPart' a una lista de SKOSConcept.
      body_parts puede ser None, string o lista de strings.
      """
      if not body_parts:
         # Si es None, vacío o equivalente
         return []
      if isinstance(body_parts, str):
         # Si es un único string, creamos una sola instancia
         return [SKOSConcept(prefLabel=body_parts)]
      if isinstance(body_parts, list):
         # Si es una lista de strings
         return [SKOSConcept(prefLabel=bp) for bp in body_parts]
      # Si llega algo que no es ni string ni lista
      raise ValueError(f"Valor inesperado en bodyPart: {body_parts}")

   dcat_dataset["hasImageBodyPart"] = convert_body_parts_to_skos(dataset["bodyPart"])
   
   dcat_dataset["accessURL"] = f"https://chaimeleon-eu.i3m.upv.es/dataset-service/datasets/{str(dataset['id'])}/details"

   
   # Eliminación de columnas innecesarias y renombrado de columnas
   for item in [
      "authorName", "authorId", "public", "pids", "previousId", "nextId", "contactInfo", "license",
      "id", "name", "project", "creationDate", "purpose", "collectionMethod", "draft", 
      "invalidated", "corrupted", "lastIntegrityCheck", "studiesCount", "subjectsCount", "ageLow", 
      "ageHigh", "ageUnit", "ageNullCount", "sex", "sexCount", "diagnosisYearLow", "diagnosisYearHigh", 
      "diagnosisYearNullCount", "bodyPart", "bodyPartCount", "modality", "modalityCount",  
      "manufacturer", "manufacturerCount", "seriesTags", "sizeInBytes", "description", "version", "type", "diagnosis", "diagnosisCount", "intendedPurpose", "hasImageBodyPart"
   ]: 
      if item in dcat_dataset: dcat_dataset.pop(item) 


   dcat_resource = DCATResourceNxt(**dcat_dataset)
   serialized_graph = dcat_resource.to_graph(URIRef(dcat_resource.identifier[0])).serialize()
   LOG.info(serialized_graph)
   
   dcat_fields = DCATResourceNxt.annotate_model()
   LOG.info(dcat_fields.fields_description())
   return serialized_graph
