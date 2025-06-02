import uuid
from sempyro.dcat import DCATDataset
import pprint
from sempyro import LiteralField, RDFModel
from rdflib import URIRef, Namespace
import logging
from .dataset_ext import DCATResourceNxt, SKOSConcept
import json
from typing import List
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

def toDCAT(dataset): # Transformaciones de los campos a sus correspondientes datatypes
   LOG = logging.root
   LOG.info(f"Translating dataset: {dataset}")
   dcat_dataset = dict()
###https://healthdcat-ap.github.io/###
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
         
   
   dcat_dataset["title"] = [LiteralField(value=dataset["name"])]
   dcat_dataset["description"] = [dataset["description"]]
   provenance_value = dataset["provenance"]
   if isinstance(provenance_value, str):
      LOG.warning(f"Ha entrado al IF de PROVENANCE: {provenance_value}")
      # Here, we're assuming the string should be mapped to the 'label' field.
      dcat_dataset["provenance"] = {"label": provenance_value}
   else:
      dcat_dataset["provenance"] = provenance_value

   dcat_dataset["intendedPurpose"] = dataset["purpose"]
   # dcat_dataset["imageCreationYear"] = dataset["creationDate"] #falta startDate y end date, necesito un periodo; Pau está mirando en los dicom para sacar start date
   # dcat_dataset["graphicalCoverage"] = 
   dcat_dataset["contact_point"] = parse_contact_info(dataset["contactInfo"])


   # Añadir tal cual los campos del ejemplo de valia hardcodeados para los de chaimeleon,
   # pero revisar en el eucaim node porque sí que hay datos de distintos proveedores y hay que ver de dónde sacar 
   # todos estos parámetros 
   # dcat_dataset["publisher"] = [dataset["project"]]
# dct:publisher [ a foaf:Organization;
#     locn:address [ a locn:Address;
#     foaf:name “HULAFE";
#     foaf:mbox <mailto:info.chaimeleon-eu@i3m.upv.es>;
#     foaf:homepage <https://www.upv.es>;
# ];
# ];

##Asegurarnos de que es este realmente el formato correcto
################## ENVIAR PREGUNTA A VALIA theme 
   # dcat_dataset["theme"] = "http://publications.europa.eu/resource/authority/data-theme/HEAL"
# Suponiendo que SKOSConcept tiene un argumento 'uri' para inicializar la URL
   theme_uri = "http://publications.europa.eu/resource/authority/data-theme/HEAL"
   dcat_dataset["theme"] = [
      SKOSConcept(
         uri=theme_uri,
         prefLabel=theme_uri
      )
   ]

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
   # def to_provenance_statements(provenance):
   #  """
   #  Convierte el valor de 'provenance' (que es el label) en una instancia de ProvenanceStatementModel.
   #  No se asigna un identificador.
   #  """
   #  if not provenance:
   #      return None  # O devuelve "" o [] según lo que espere el resto de la aplicación

   #  if isinstance(provenance, str):
   #      return ProvenanceStatementModel(
   #          label=provenance
   #      )

   #  raise ValueError(f"Valor inesperado en provenance: {provenance}")

# Asignación al diccionario dcat_dataset:


## Saber si es válido /HEALTH y ver campo añadido por pau. y ver que tipo 
   dcat_dataset["type"] = "https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#Dataset"

   dcat_dataset["version"] = [dataset["version"]]
   
   
   def extract_age(value):
    if isinstance(value, list) and value:  # If it's a list and not empty
        return int(value[0]) if value[0] is not None else None
    elif isinstance(value, int):  # If it's already an integer
        return value
    else:
        return None 
    
   dcat_dataset["ageLow"] = extract_age(dataset.get("ageLow"))
   dcat_dataset["ageHigh"] = extract_age(dataset.get("ageHigh"))
## Revisar LegalBasis
   dcat_dataset["legalBasis"] = "http://data.europa.eu/eli/reg/2022/868/oj"



## TO DO definir el  quality annotation y comprender de donde obtenerlo
##   dcat_dataset["qualityLabel"] = 
   def convert_collection_method_to_skos(collection_method):
      """
      Convierte el valor en la columna 'collectionMethod' a una lista de SKOSConcept.
      collection_method puede ser None, string o lista de strings.
      """
      if not collection_method:
         return []
      if isinstance(collection_method, str):
         return [SKOSConcept(prefLabel=collection_method)]
      if isinstance(collection_method, list):
         return [SKOSConcept(prefLabel=cm) for cm in collection_method]
      raise ValueError(f"Valor inesperado en collectionMethod: {collection_method}")

   dcat_dataset["collectionMethod"] = convert_collection_method_to_skos(dataset["collectionMethod"])


   # dcat_dataset["conforms_to"] = dataset["license"]["url"]

   def convert_sex_to_skos(sex):
      """
      Convierte el valor en la columna 'sex' a una lista de SKOSConcept.
      sex puede ser None, string o lista de strings.
      """
      if not sex:
         return []
      if isinstance(sex, str):
         return [SKOSConcept(prefLabel=sex)]
      if isinstance(sex, list):
         return [SKOSConcept(prefLabel=s) for s in sex]
      raise ValueError(f"Valor inesperado en sex: {sex}")

   dcat_dataset["birthsex"] = convert_sex_to_skos(dataset["sex"])


   
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

   
   # # Eliminación de columnas innecesarias y renombrado de columnas
   # for item in [
   #    "authorName", "authorId", "public", "pids", "previousId", "nextId", "contactInfo", "license",
   #    "id", "name", "project", "creationDate", "purpose", "collectionMethod", "draft", 
   #    "invalidated", "corrupted", "lastIntegrityCheck", "studiesCount", "subjectsCount", "ageLow", 
   #    "ageHigh", "ageUnit", "ageNullCount", "sex", "sexCount", "diagnosisYearLow", "diagnosisYearHigh", 
   #    "diagnosisYearNullCount", "bodyPart", "bodyPartCount", "modality", "modalityCount",  
   #    "manufacturer", "manufacturerCount", "seriesTags", "sizeInBytes", "description", "version", "type", "diagnosis", "diagnosisCount", "intendedPurpose", "hasImageBodyPart"
   # ]: 
   #    if item in dcat_dataset: dcat_dataset.pop(item) 


   dcat_resource = DCATResourceNxt(**dcat_dataset)

   for field, value in iter(dcat_resource):
     if value:
        LOG.debug("#field# " + json.dumps(field) )
        if not isinstance(value, List):
            value = [value]
        for item in value:
            if issubclass(type(item), RDFModel):
                LOG.debug("#fieldvalueitem# " )#+ json.dumps(item))
                for k in item.model_config:
                    LOG.debug("#k# " + k)
                    LOG.debug("#v# " + json.dumps(item.model_config[k]))

   serialized_graph = dcat_resource.to_graph(URIRef(dcat_resource.identifier[0])).serialize()
   LOG.info(serialized_graph)
   
   dcat_fields = DCATResourceNxt.annotate_model()
   LOG.info(dcat_fields.fields_description())
   return serialized_graph

