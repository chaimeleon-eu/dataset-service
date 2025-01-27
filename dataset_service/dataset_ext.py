import logging
from abc import ABCMeta
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import List, Union

from pydantic import BaseModel, Field, AnyHttpUrl, EmailStr, ConfigDict, Field
from rdflib import DCAT, DCTERMS, ODRL2, PROV, URIRef

from sempyro import LiteralField, RDFModel
from sempyro.foaf import Agent
from sempyro.namespaces import ADMS, ADMSStatus, DCATv3, DQV
from sempyro.odrl import ODRLPolicy
from sempyro.utils.validator_functions import date_handler, force_literal_field
from sempyro.vcard import VCard

logger = logging.getLogger("__name__")


class Status(Enum):
    Completed = ADMSStatus.Completed
    Deprecated = ADMSStatus.Deprecated
    UnderDevelopment = ADMSStatus.UnderDevelopment
    Withdrawn = ADMSStatus.Withdrawn

class SKOSConcept(BaseModel):
    type: str = Field(default="skos:Concept", const=True)
    prefLabel: str = Field(alias="skos:prefLabel")

class AccessRights(Enum):
    public = URIRef("http://publications.europa.eu/resource/authority/access-right/PUBLIC")
    restricted = URIRef("http://publications.europa.eu/resource/authority/access-right/RESTRICTED")
    non_public = URIRef("http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC")

class Address(BaseModel):
    type: str = Field(default="locn:Address", const=True)
    name: str = Field(alias="foaf:name")
    mbox: EmailStr = Field(alias="foaf:mbox")
    homepage: AnyHttpUrl = Field(alias="foaf:homepage")

class Organization(BaseModel):
    type: str = Field(default="foaf:Organization", const=True)
    address: Address = Field(alias="locn:address")

class Purpose(BaseModel):
    type: str = Field(default="dpv:Purpose", const=True)
    description: str = Field(alias="dct:description")

class QualityAnnotation(RDFModel):
    """
    Representa una anotación de calidad, conforme a dqv:QualityAnnotation.
    Subclase de oa:Annotation, con la motivación oa:motivatedBy dqv:qualityAssessment.
    """
    
    type: str = Field(
        default="dqv:QualityAnnotation",
        const=True,
        description="Tipo de anotación de calidad conforme a DQV."
    )

    hasTarget: AnyHttpUrl = Field(
        alias="oa:hasTarget",
        description="URI del recurso (dataset/distribution) al que se asigna la anotación de calidad."
    )

    hasBody: AnyHttpUrl = Field(
        alias="oa:hasBody",
        description="URI del recurso (certificado o documento) que describe la calidad."
    )

    motivatedBy: str = Field(
        default="dqv:qualityAssessment",
        alias="oa:motivatedBy",
        description="Motivación de la anotación, en este caso 'dqv:qualityAssessment'."
    )


class DCATResourceNxt(RDFModel, metaclass=ABCMeta):
    """Resource published or curated by a single agent. Abstract class"""
    model_config = ConfigDict(arbitrary_types_allowed=True,
                              use_enum_values=True,
                              json_schema_extra={
                                  "$ontology": "https://www.w3.org/TR/vocab-dcat-3/",
                                  "$namespace": str(DCAT),
                                  "$IRI": DCAT.Resource,
                                  "$prefix": "dcat"
                              }
                              )
    
    title: List[LiteralField] = Field(
        description="A name given to the resource.",
        rdf_term=DCTERMS.title,
        rdf_type="rdfs_literal")

    description: List[Union[str, LiteralField]] = Field(
        default=None,
        description="A description of the resource.",
        rdf_term=DCTERMS.description,
        rdf_type="rdfs_literal")
    
    contact_point: List[Union[AnyHttpUrl, VCard, Agent]] = Field(
        default=None,
        description="Relevant contact information for the cataloged resource. Use of vCard is recommended",
        rdf_term=DCAT.contactPoint,
        rdf_type="uri")
    
    #Revisar formato uri foaf organization
    publisher: Organization = Field(
         default=None,
         description="The entity responsible for making the resource available.",
         rdf_term=DCTERMS.publisher,
         rdf_type="uri")

    theme: List[SKOSConcept] = Field(
        default=None,
        description="A main category of the resource.",
        rdf_term=DCAT.theme,
        rdf_type="uri")
    
    identifier: List[Union[str, LiteralField]] = Field(
        default=None,
        description="A unique identifier of the resource being described or cataloged.",
        rdf_term=DCTERMS.identifier,
        rdf_type="rdfs_literal")
    
    access_rights: AccessRights = Field(
        description="The access rights of the resource",
        rdf_term=DCTERMS.accessRights,
        rdf_type="uri")
    
    rights: List[Union[str, LiteralField]] = Field(
        default=None,
        description="Information about rights held in and over the resource.",
        rdf_term=DCTERMS.rights,
        rdf_type="rdfs_literal")
    
    applicableLegislation: List[Union[str, LiteralField]] = Field(
        default=None,
        description="The legislation that applies to the resource.",
        rdf_term=DCAT.applicableLegislation,
        rdf_type="rdfs_literal")
    
    provenance: List[Union[str, LiteralField]] = Field(
        default=None,
        description="A statement concerning the source and lineage of the Dataset.",
        rdf_term=PROV.wasDerivedFrom,
        rdf_type="rdfs_literal")
    
    type: List[SKOSConcept] = Field(
        default=None,
        description="The nature or genre of the resource.",
        rdf_term=DCTERMS.type,
        rdf_type="uri")
    
    version: List[Union[str, LiteralField]] = Field(
        default=None,
        description="A version of the resource.",
        rdf_term=DCTERMS.hasVersion,
        rdf_type="rdfs_literal")
    
    ageLow: int = Field(
        default=None,
        description="The lower age of the intended audience of the resource.",
        rdf_term=DCAT.ageLow,
        rdf_type="xsd:integer")
    
    ageHigh: int = Field(     
        default=None,
        description="The upper age of the intended audience of the resource.",
        rdf_term=DCAT.ageHigh,
        rdf_type="xsd:integer")
    
    nbrOfStudies: int = Field(   
        default=None,
        description="The number of studies in the resource.",
        rdf_term=DCAT.nbrOfStudies,
        rdf_type="xsd:integer")
    
    nbrOfSubjects: int = Field(
        default=None,
        description="The number of subjects in the resource.",
        rdf_term=DCAT.nbrOfSubjects,
        rdf_type="xsd:integer")
    

    legalBasis: List[Union[str, LiteralField]] = Field(
        default=None,
        description="The legal basis used to justify processing of personal data.",
        rdf_term=DCAT.legalBasis,
        rdf_type="rdfs_literal")
    
    # intendedPurpose: List[Purpose] = Field(
    #     default=None,
    #     description="free text statement of the purpose of the processing of personal data.",
    #     ##rdf_term="chaimeleon:intendedPurpose",
    #     rdf_type="uri")

    hasQualityAnnotation: List[QualityAnnotation] = Field(
        default=None,
        description=(
            "Refers to a quality annotation. Quality annotation can be applied "
            "to any resource, e.g., a dataset, linkset, or distribution. "
            "In DQV context, the subject is typically a dcat:Dataset or "
            "dcat:Distribution."
        ),
        rdf_term=DQV.hasQualityAnnotation,
        #rdf_term="http://www.w3.org/ns/dqv#hasQualityAnnotation"
        rdf_type="uri"
    )
    
    ## health data access body

    collectionMethod: List[SKOSConcept] = Field(
        default=None,
        description="The method used to collect the resource.",
        rdf_term=DCAT.collectionMethod,
        rdf_type="uri")


    # conforms_to: AnyHttpUrl = Field(
    #     default=None,
    #     description="An established standard to which the described resource conforms.",
    #     rdf_term=DCTERMS.conformsTo,
    #     rdf_type="uri")
    



    
    sex: List[SKOSConcept] = Field(
        default=None,
        description="The sex of the subjects in the resource.",
        rdf_term="skos:Concept",
        rdf_type="xsd:string")

    condition: List[SKOSConcept] = Field(
        default=None,
        description="The condition of the subjects in the resource.",
        rdf_term="skos:Concept",
        rdf_type="xsd:string")
    
    imageModality: List[SKOSConcept] = Field(
        default=None,
        description="The imaging modality of the resource.",
        rdf_term="skos:Concept",
        rdf_type="xsd:string")
    
    bodyPart: List[SKOSConcept] = Field(
        default=None,
        description="The body part(s) involved in the resource.",
        rdf_term="skos:Concept",
        rdf_type="xsd:string")
    
    accessURL: AnyHttpUrl = Field(
        default=None,
        description="The URL to access the resource.",
        rdf_term=DCAT.accessURL,
        rdf_type="uri")
    

    # creator: List[Union[AnyHttpUrl, VCard, Agent]] = Field(
    #     default=None,
    #     description="The entity responsible for producing the resource. Resources of type foaf:Agent are "
    #                 "recommended as values for this property.",
    #     rdf_term=DCTERMS.creator,
    #     rdf_type="uri")
    


    def convert_to_literal(cls, value: List[Union[str, LiteralField]]) -> List[LiteralField]:
        return [force_literal_field(item) for item in value]
    @classmethod
    def date_validator(cls, value):
        return date_handler(value)


if __name__ == "__main__":
    json_models_folder = Path(Path(__file__).parents[2].resolve(), "models", "dcat")
    DCATResourceNxt.save_schema_to_file(Path(json_models_folder, "DCATResourceNxt.json"), "json")
    DCATResourceNxt.save_schema_to_file(Path(json_models_folder, "DCATResourceNxt.yaml"), "yaml")