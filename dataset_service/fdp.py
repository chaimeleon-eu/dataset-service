from sempyro.dcat import DCATDataset
import pandas as pd
import pprint
from rdflib import URIRef
import logging



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

   # Apply the access_rights mapping
   df["access_rights"] = df["public"].apply(map_access_rights)

   df["conforms_to"] = df["license"].apply(lambda x: x["url"])

   # 'contact_point': debe ser una lista de tipo 'Url' o 'VCard' (uso de VCard para nombre y email)
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


    # Aplicar la función a la columna 'contactInfo'
   df["contact_point"] = df["contactInfo"].apply(parse_contact_info)
   #df["contact_point"] = df["contactInfo"].apply(lambda x: [{"vcard:fn": x.split(" ")[0], "vcard:email": x.split(" ")[1][1:-1]}])

   # 'creator': lista de tipo 'VCard' o 'Agent' (nombre completo y ID del autor)
   # Esta linea en teoría transforma en formato VCARD, si no funciona poner en formato original ("James Gordon (james@email.com)")
   # df["creator"] = df.apply(lambda x: [{"vcard:fn": x["authorName"], "vcard:hasUID": x["authorId"]}], axis=1)
   # df["creator"] = df.apply(
   #  lambda x: [
   #      # Fallback to license URL if authorId-based URL is not available
   #      {"url": x["license"]["url"]} 
        
   #      # VCard format with name and identifier (instead of email)
   #      {
   #          "vcard:fn": x["authorName"],
   #          "vcard:hasUID": x["authorId"],
   #          "vcard:identifier": x["authorId"]  # Replace email with identifier
   #      },
        
   #      # Agent format with name and identifier
   #      {"name": x["authorName"], "identifier": x["authorId"]}
   #  ],
   #  axis=1
   # )

   # 'description': lista de tipo 'LiteralField'
   df["description"] = df["description"].apply(lambda x: [x])

   # 'distribution': lista de tipo 'Url' para DOI y URL del dataset
   #df["distribution"] = "An available distribution of the dataset."#df["pids"].apply(lambda x: [x["urls"]["zenodoDoi"], x["urls"]["custom"]])
   
   # df["first"] = "The first resource in an ordered collection or series of resources, to which the current resource belongs."
   
   # df["frecuency"] = "The frequency at which a dataset is published."
   # # 'has_current_version': debe ser tipo 'Url' (URL que apunta a la versión actual)
   # df["has_current_version"] = "This resource has a more specific, versioned resource with equivalent content [PAV]." #df["nextId"].apply(lambda x: [f"https://myDatasetsDB.com/ds/{x}"])

   # df["has_part"] = "A related resource that is included either physically or logically in the described resource." 

   # df["has_version"] = 'This resource has a more specific, versioned resource'

   # 'identifier': lista de tipo 'rdfs_literal' o string
   df["identifier"] = df["id"].apply(lambda x: [str(x)])

   # 'in_series': lista de tipo 'Url', apunta a las versiones previas y posteriores
   df["in_series"] = df.apply(lambda x: [f"https://myDatasetsDB.com/ds/{x['previousId']}", f"https://myDatasetsDB.com/ds/{x['nextId']}"], axis=1)

   # df["is_referenced_by"] = 'A related resource that references, cites, or otherwise points to the described resource.'
   # # 'license': debe ser tipo 'Url'
   # df["keyword"] = 'A word or phrase used to describe the resource.'

   # df["landing_page"] = 'A Web page that can be navigated to in a Web browser to gain access to the dataset, its distributions, additional information, and more.'
   
   # df["language"] = 'The language of the resource.'

   # df["last"] = 'The last resource in an ordered collection or series of resources, to which the current resource belongs.'

   df["license"] = df["license"].apply(lambda x: x["url"])

   # 'previous': lista de tipo 'Url'
   df["previous"] = df["previousId"].apply(lambda x: [f"https://myDatasetsDB.com/ds/{x}"])

   # df["previous_version"] = 'The previous version of a resource in a lineage [PAV].'
   # 'rights': debe ser tipo 'LiteralField' o 'Url'

   # df["publisher"] = 'An entity responsible for making the resource available.'

   # df["qualified_attribution"] = 'Link to an Agent that is responsible for making the resource available.'

   # df["qualified_relation"] = 'Link to a description of a relationship with another resource.'

   # df["relation"] = "A resource with an unspecified relationship to the cataloged resource."

   # df["release_date"] = 'The date of formal issuance (e.g., publication) of the dataset.'

   # df["replaces"] = 'A related resource that is supplanted, displaced, or superseded by the described resource.'

   df["rights"] = df["license"]

   # df["spatial"] = 'Geographic area covered by the dataset.'

   # df["spatial_resolution"] = 'Minimum spatial separation resolvable in a dataset, measured in meters.'

# df["status"] = 'The status of the resource in the context of a particular workflow process [VOCAB-ADMS].'
   def infer_status(row):
       # Mapping based on the flags you provided
      if row["invalidated"]:
        return URIRef("http://purl.org/adms/status/Withdrawn")  # Dataset is invalidated, mark it as Withdrawn
      elif row["corrupted"]:
        return URIRef("http://purl.org/adms/status/Withdrawn")  # Dataset is corrupted, also Withdrawn
      elif row["draft"] :
        return URIRef("http://purl.org/adms/status/UnderDevelopment")  # Draft or creating status means still in development
      elif row["public"]:
        return URIRef("http://purl.org/adms/status/Completed")  # If public and valid, mark as Completed
      else:
        return URIRef("http://purl.org/adms/status/UnderDevelopment")  # Default to UnderDevelopment if none apply

   # Apply the function to the dataframe to generate the status field
   df["status"] = df.apply(infer_status, axis=1)
   # df["temporal_coverage"] = 'The temporal period that the dataset covers.'

   # df["temporal_resolution"] = 'Minimum time period resolvable in the dataset.'

   # df["theme"] = 'A main category of the resource. A resource can have multiple themes.'

   # df["title"] = 'A name given to the resource.'
   df["title"] = df["name"].apply(lambda x: [x])

   # df["type"] = 'The nature or genre of the resource.'

   # df["update_date"] = 'Most recent date on which the resource was changed, updated or modified.'

   #df["version"] = 'The version indicator (name or identifier) of a resource.'
   df["version"] = df["version"].apply(lambda x: [x])

   # df["version_notes"] = 'A description of changes between this version and the previous version of the resource [VOCAB-ADMS].'

   # df["was_generated_by"] = 'An activity that generated, or provides the business context for, the creation of the dataset.'

   # Eliminación de columnas innecesarias y renombrado de columnas
   df.drop(columns=[
      "authorName", "authorId", "public", "pids", "previousId", "nextId", "contactInfo", "license",
      "id", "name", "project", "creationDate", "purpose", "collectionMethod", "draft", 
      "invalidated", "corrupted", "lastIntegrityCheck", "studiesCount", "subjectsCount", "ageLow", 
      "ageHigh", "ageUnit", "ageNullCount", "sex", "sexCount", "diagnosisYearLow", "diagnosisYearHigh", 
      "diagnosisYearNullCount", "bodyPart", "bodyPartCount", "modality", "modalityCount", 
      "manufacturer", "manufacturerCount", "seriesTags", "sizeInBytes"
   ], inplace=True)
   
   datasets = df.to_dict('records')   
   dcat_datasets = [DCATDataset(**x) for x in datasets]   
   for item in dcat_datasets:
      serialized_graph = item.to_graph(URIRef(item.identifier[0])).serialize()
      print(serialized_graph)
      serialized_graphs.append(serialized_graph)
   
   
   dcat_fields = DCATDataset.annotate_model()
   pprint.pprint(dcat_fields.fields_description())
   return serialized_graphs