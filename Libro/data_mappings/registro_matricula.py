from django.db import connection


def numero_correlativo_matricula():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT a.personId,b.Identifier NumeroMatricula  FROM Person a 
inner join PersonIdentifier b on a.personId=b.personId and b.RefPersonIdentificationSystemId=55""")
        res = [x for x in res]
        return res, ["personid", "Numero de Matricula"]


def run():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.personId,b.Identifier   from Person a 
inner join PersonIdentifier b on a.personId=b.personId and b.RefPersonIdentificationSystemId=51
        """)
        res = [x for x in res]
        return res, []


def ipe():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT a.personId,b.Identifier FROM Person a inner join PersonIdentifier b on a.personId=b.personId and b.RefPersonIdentificationSystemId=52
        """)
        res = [x for x in res]
        return res, ["person id", "identificador"]


def primer_nombre_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT FirstName FROM Person join OrganizationPersonRole using(PersonId) where OrganizationPersonRole.RoleId = 6
        """)
        res = [x for x in res]
        return res, ["Primer nombre"]


def segundo_nombre_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT MiddleName FROM Person join OrganizationPersonRole using(PersonId) where OrganizationPersonRole.RoleId = 6
        """)
        res = [x for x in res]
        return res, ["Segundo nombre"]


def apellido_paterno_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT LastName FROM Person join OrganizationPersonRole using(PersonId) where OrganizationPersonRole.RoleId = 6
        """)
        res = [x for x in res]
        return res, ["Apellido"]


def apellido_materno_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT SecondLastName FROM Person join OrganizationPersonRole using(PersonId) where OrganizationPersonRole.RoleId = 6
        """)
        res = [x for x in res]
        return res, ["Segundo apellido"]


def sexo():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT RefSex.Description FROM Person join RefSex using(RefSexId)
        """)
        res = [x for x in res]
        return res, ["Sexo"]


def fecha_nacimiento():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT Birthdate FROM Person
        """)
        res = [x for x in res]
        return res, ["Fecha de nacimiento"]


def nivel():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        
        """)
        res = [x for x in res]
        return res, []


def curso():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []


def local_escolar():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select distinct StreetNumberAndName,city Ciudad,CountyName Comuna from LocationAddress
        """)
        res = [x for x in res]
        return res, ["Numero y nombre de la calle", "Ciudad", "Comuna"]


def fecha_matricula():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select b.personid,a.StatusStartDate from PersonStatus a
inner join person b on a.personId=b.PersonId
        """)
        res = [x for x in res]
        return res, ["person id", "Fecha de matrícula"]
def fecha_retiro_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select b.personid,a.StatusEndDate from PersonStatus a
inner join person b on a.personId=b.PersonId
        """)
        res = [x for x in res]
        return res, ["person id", "Fecha retiro"]


def motivo_retiro_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select persona.personId,
numeroMatricula.Identifier NumMatricula,
fechaMatricula.StatusStartDate FechaMatricula ,
persona.FirstName PrimerNombre,
persona.MiddleName SegundoNombre,
persona.LastName ApellidoPaterno,
persona.SecondLastName ApellidoMaterno,
fechaMatricula.recordEndDateTime FechaRetiro,
fechaMatricula.Description MotivoRetiro 
from Person persona 
inner join PersonIdentifier numeroMatricula on persona.personId=numeroMatricula.personId and numeroMatricula.RefPersonIdentificationSystemId=55
inner join personstatus fechaMatricula on persona.personid=fechamatricula.personid
        """)
        res = [x for x in res]
        return res, ["person id", "número de matrícula", "Fecha de matrícula", "Primer nombre", "Segundo nombre", "Apellido paterno", "Apellido materno", "Fecha retiro", "Motivo de retiro"]


def domicilio_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.personId,b.StreetNumberAndName from person a
inner join PersonAddress b on a.personId=b.personId
        """)
        res = [x for x in res]
        return res, ["person id", "Número y nombre de calle"]


def observaciones():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT p.PersonId, group_concat(ps.Description, "", "") FROM Person p
INNER JOIN PersonStatus ps
ON p.PersonId = ps.PersonId
GROUP BY p.PersonId
        """)
        res = [x for x in res]
        return res, ["person id", "Descripcion"]


def domicilio_padre():
    with connection.cursor() as cursor:
        res = cursor.execute("""
SELECT relacion.firstname,
relacion.MiddleName,
relacion.LastName,
relacion.SecondLastName,
direccion.StreetNumberAndName,
comuna.Description,
telefono.TelephoneNumber,
email.emailAddress,
NivelEducacionalMAE.Description EducacionNivel
FROM person a
inner join PersonRelationship b on a.personid=b.personid and b.refpersonrelationshipid=8
inner join refpersonrelationship c on b.refpersonrelationshipid=c.refpersonrelationshipid
inner join person relacion on b.relatedpersonid=relacion.personid
left join PersonAddress direccion on relacion.personid=direccion.personId
left join refcounty comuna on direccion.RefCountyId=comuna.RefCountyId
left join PersonTelephone telefono on relacion.personid=telefono.personid
left join PersonEmailAddress email on relacion.personid=email.personid
left join PersonDegreeOrCertificate NivelEducacional on relacion.personid=Niveleducacional.personid
left join RefDegreeOrCertificateType NivelEducacionalMAE 
on Niveleducacional.RefDegreeOrCertificateTypeid=NivelEducacionalMAE.RefDegreeOrCertificateTypeid
        """)
        res = [x for x in res]
        return res, []


def telefono_padre():
    return domicilio_padre()


def email_padre():
    return domicilio_padre()


def domicilio_madre():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        
select relacion.firstname,
relacion.MiddleName,
relacion.LastName,
relacion.SecondLastName,
direccion.StreetNumberAndName,
comuna.Description,
telefono.TelephoneNumber,
email.emailAddress,
NivelEducacionalMAE.Description EducacionNivel
from person a
inner join PersonRelationship b on a.personid=b.personid and b.refpersonrelationshipid=19
inner join refpersonrelationship c on b.refpersonrelationshipid=c.refpersonrelationshipid
inner join person relacion on b.relatedpersonid=relacion.personid
left join PersonAddress direccion on relacion.personid=direccion.personId
left join refcounty comuna on direccion.RefCountyId=comuna.RefCountyId
left join PersonTelephone telefono on relacion.personid=telefono.personid
left join PersonEmailAddress email on relacion.personid=email.personid
left join PersonDegreeOrCertificate NivelEducacional on relacion.personid=Niveleducacional.personid
left join RefDegreeOrCertificateType NivelEducacionalMAE on Niveleducacional.RefDegreeOrCertificateTypeid=NivelEducacionalMAE.RefDegreeOrCertificateTypeid
        """)
        res = [x for x in res]
        return res, []


def telefono_madre():
    return domicilio_madre()


def email_madre():
    return domicilio_madre()


def presedencia():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT DISTINCT p.PersonId, rpst.Description
FROM
Person p
INNER JOIN PersonStatus ps
ON p.PersonId=ps.PersonId
INNER JOIN RefPersonStatusType rpst
ON rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
AND rpst.RefPersonStatusTypeId IN (27,28)
        """)
        res = [x for x in res]
        return res, []
def nivel_educacional_padre():
    return domicilio_padre()
def nivel_educacional_madre():
    return domicilio_madre()
def persona_con_quien_vive():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.PersonId,c.Description from person a
inner join PersonRelationship b on a.personId=b.personId
inner join RefPersonRelationship c on b.RefPersonRelationshipId=c.RefPersonRelationshipId
        """)
        res = [x for x in res]
        return res, []
def tipo_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT DISTINCT p.PersonId, rpst.Description
FROM
Person p
INNER JOIN PersonStatus ps
ON p.PersonId=ps.PersonId
INNER JOIN RefPersonStatusType rpst
ON rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
AND rpst.RefPersonStatusTypeId IN (24,31,25,26,5)
        """)
        res = [x for x in res]
        return res, []
def doc_pais_origen():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.personId,b.Identifier   from Person a 
inner join PersonIdentifier b on a.personId=b.personId and b.RefPersonIdentificationSystemId=53
        """)
        res = [x for x in res]
        return res, []
def tipo_matricula():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def numero_fecha_resolucion():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def rbd():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select SUBSTR(ShortName, 4, 8) RBD from organization
 where reforganizationtypeid=10  --/* el formato ej es RBD09599*/
        """)
        res = [x for x in res]
        return res, []
def nombre_establecimiento():
    with connection.cursor() as cursor:
        res = cursor.execute("""
         select name NombreEstablecimiento from organization
where reforganizationtypeid=10
        """)
        res = [x for x in res]
        return res, ["NombreEstablecimiento"]
def modalidad():
    with connection.cursor() as cursor:
        res = cursor.execute("""
         select name Modalidad /*deben ser solo 3 regular, especial, adulto*/ from organization
where reforganizationtypeid=38
        """)
        res = [x for x in res]
        return res, []
def anio_escolar():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select CalendarYear from organizationcalendar limit 1 /*solo un año por archivo*/
        """)
        res = [x for x in res]
        return res, []
def primer_nombre_profesor_jefe():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select DISTINCT
                 curso.OrganizationId as OrganizationIdDelCurso
                , profesorJefe.apellidoPaternoDocenteLiderCurso
                , profesorJefe.apellidoMaternoDocenteLiderCurso
                , profesorJefe.primerNombreDocenteLiderCurso
                , profesorJefe.otrosNombresDocenteLiderCurso
                , profesorJefe.runDocenteLiderCurso

        FROM Organization as curso
        INNER JOIN OrganizationRelationship as rsCurso on curso.OrganizationId=rsCurso.OrganizationId
        OUTER LEFT JOIN (
                SELECT 
                        OrganizationPersonRoleId
                        , OrganizationId
                        , PersonId
                        , LastName as 'apellidoPaternoDocenteLiderCurso'
                        , SecondLastName as 'apellidoMaternoDocenteLiderCurso'
                        , FirstName as 'primerNombreDocenteLiderCurso'
                        , MiddleName as 'otrosNombresDocenteLiderCurso'
                        , runDocenteLiderCurso
                FROM K12StaffAssignment
                INNER JOIN OrganizationPersonRole USING(OrganizationPersonRoleId)
                INNER JOIN (
                                        SELECT DISTINCT 
                                                Person.PersonId
                                                ,Person.LastName
                                                ,Person.SecondLastName
                                                ,Person.FirstName
                                                ,Person.MiddleName
                                                ,rut.Identifier as RunDocenteLiderCurso 
                                        FROM Person 
                                        INNER JOIN PersonIdentifier rut ON rut.PersonId = Person.PersonId AND rut.RefPersonIdentificationSystemId = 51 
                                ) USING(PersonId)
                                WHERE RefTeachingAssignmentRoleId = 1
                        ) profesorJefe ON OrganizationIdDelCurso = profesorJefe.OrganizationId
        
                WHERE curso.RefOrganizationTypeId = 21
        """)
        res = [x for x in res]
        return res, []
def segundo_nombre_profesor_jefe():
    return primer_nombre_profesor_jefe()
def apellido_paterno_profesor_jefe():
    return primer_nombre_profesor_jefe()
def apellido_materno_profesor_jefe():
    return primer_nombre_profesor_jefe()
def domicilio_estudiante_comuna():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def primer_nombre_apoderado_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select relacion.firstname,
relacion.MiddleName,
relacion.LastName,
relacion.SecondLastName,
direccion.StreetNumberAndName,
comuna.Description,
telefono.TelephoneNumber,
email.emailAddress,
NivelEducacionalMAE.Description EducacionNivel
from person a
inner join PersonRelationship b on a.personid=b.personid and b.refpersonrelationshipid=31
inner join refpersonrelationship c on b.refpersonrelationshipid=c.refpersonrelationshipid
inner join person relacion on b.relatedpersonid=relacion.personid
left join PersonAddress direccion on relacion.personid=direccion.personId
left join refcounty comuna on direccion.RefCountyId=comuna.RefCountyId
left join PersonTelephone telefono on relacion.personid=telefono.personid
left join PersonEmailAddress email on relacion.personid=email.personid
left join PersonDegreeOrCertificate NivelEducacional on relacion.personid=Niveleducacional.personid
left join RefDegreeOrCertificateType NivelEducacionalMAE on Niveleducacional.RefDegreeOrCertificateTypeid=NivelEducacionalMAE.RefDegreeOrCertificateTypeid
        """)
        res = [x for x in res]
        return res, []
def segundo_nombre_apoderado_tutor():
    return primer_nombre_apoderado_tutor()
def apellido_paterno_apoderado_tutor():
    return primer_nombre_apoderado_tutor()
def apellido_materno_apoderado_tutor():
    return primer_nombre_apoderado_tutor()
def telefono_apoderado_tutor():
    return primer_nombre_apoderado_tutor()
def email_apoderado_tutor():
    return primer_nombre_apoderado_tutor()
def datos_biologicos_salud_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select * from Person persona
left join PersonHealth salud on persona.personid=salud.personid
left join PersonAllergy alergia on persona.personid=alergia.personid
        """)
        res = [x for x in res]
        return res, []
def n_resolucion_fecha_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        SELECT DISTINCT p.PersonId, rpst.Description, ps.StatusStartDate
FROM
Person p
INNER JOIN PersonStatus ps
ON p.PersonId=ps.PersonId
INNER JOIN RefPersonStatusType rpst
ON rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
AND rpst.RefPersonStatusTypeId IN (27,28)
        """)
        res = [x for x in res]
        return res, []
def intercambio_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
select b.personid,c.Description,b.docnumber numeroResolucion,b.filescanbase64 archivo,b.StatusStartDate fechadocumento  
from PersonStatus a
inner join person b on a.personId=b.PersonId and a.RefPersonStatusTypeId in (25)
inner join RefPersonStatusType c on a.RefPersonStatusTypeid=c.RefPersonStatusTypeId
where a.recordenddatetime is null
        """)
        res = [x for x in res]
        return res, []
def otro_dato_interes_estudiante():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select b.personid,c.Description  from PersonStatus a
inner join person b on a.personId=b.PersonId and a.RefPersonStatusTypeId in (26,5)
inner join RefPersonStatusType c on a.RefPersonStatusTypeid=c.RefPersonStatusTypeId
        """)
        res = [x for x in res]
        return res, []
def modalidad_dual():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def nombre_empresa():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select empresa.name NombreEmpresa,
fonoempresa.TelephoneNumber telefonoempresa,
emailempresa.ElectronicMailAddress correoempresa,
direccionempresa.StreetNumberAndName dirempresa
 from organization empresa
left join OrganizationTelephone fonoempresa on empresa.organizationid=fonoempresa.organizationid
left join OrganizationEmail emailempresa on empresa.organizationid=emailempresa.organizationid
left join OrganizationLocation orgloca on empresa.organizationid=orgloca.OrganizationId
left join LocationAddress direccionempresa on orgloca.locationid=direccionempresa.locationid
where empresa.reforganizationtypeid=26
        """)
        res = [x for x in res]
        return res, []
def direccion_empresa():
    return nombre_empresa()
def telefono_empresa():
    return nombre_empresa()
def comuna_empresa():
    return nombre_empresa()
def email_empresa():
    return nombre_empresa()
def primer_nombre_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        --supervisor de practica/dual
        select * from organization a
inner join OrganizationPersonRole rolesupervisor on a.organizationid=rolesupervisor.organizationid and rolesupervisor.roleid=17
left join RefOrganizationType ramopractica on a.reforganizationtypeid=ramopractica.reforganizationtypeid and ramopractica.RefOrganizationTypeId=47
left join person profesorsupervisor on rolesupervisor.personid=profesorsupervisor.personid
left join PersonTelephone telefonosupervisor on profesorsupervisor.personid=telefonosupervisor.personId
left join PersonEmailAddress emailsupervisor on profesorsupervisor.personid=emailsupervisor.personid
        """)
        res = [x for x in res]
        return res, []
def segundo_nombre_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        --apoderado o tutor
        select relacion.firstname,
relacion.MiddleName,
relacion.LastName,
relacion.SecondLastName,
direccion.StreetNumberAndName,
comuna.Description,
telefono.TelephoneNumber,
email.emailAddress,
NivelEducacionalMAE.Description EducacionNivel
from person a
inner join PersonRelationship b on a.personid=b.personid and b.refpersonrelationshipid=31
inner join refpersonrelationship c on b.refpersonrelationshipid=c.refpersonrelationshipid
inner join person relacion on b.relatedpersonid=relacion.personid
left join PersonAddress direccion on relacion.personid=direccion.personId
left join refcounty comuna on direccion.RefCountyId=comuna.RefCountyId
left join PersonTelephone telefono on relacion.personid=telefono.personid
left join PersonEmailAddress email on relacion.personid=email.personid
left join PersonDegreeOrCertificate NivelEducacional on relacion.personid=Niveleducacional.personid
left join RefDegreeOrCertificateType NivelEducacionalMAE on Niveleducacional.RefDegreeOrCertificateTypeid=NivelEducacionalMAE.RefDegreeOrCertificateTypeid
        """)
        res = [x for x in res]
        return res, []
def apellido_paterno_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def apellido_materno_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def telefono_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def correo_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def rut_tutor():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        """)
        res = [x for x in res]
        return res, []
def nacionalidad():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.personid,c.Description Nacionalidad from Person a
left join PersonBirthplace b on a.personid=b.personid
left join RefCountry c on b.refcountryid=c.refcountryid
        """)
        res = [x for x in res]
        return res, []
def etnia():
    with connection.cursor() as cursor:
        res = cursor.execute("""
        select a.personid,b.Description ETNIA from person a
 left join RefTribalAffiliation b on a.RefTribalAffiliationid=b.RefTribalAffiliationid
        """)
        res = [x for x in res]
        return res, []

