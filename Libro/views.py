from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from . import merge_database
from .data_mappings import registro_matricula


# Create your views here.
def login(request):
    pass


def upload_database(request):
    if request.method == "GET":
        return render(request, "Libro/upload_database.html", {})
    elif request.method == "POST":
        print("post enter")
        file = request.FILES.get("file", None)
        merge_database.process_file(file)
        return redirect("/libro/upload_database")
    else:
        return HttpResponse("No hay implementación")


def registro_matricula_view(request):
    print("holanda jaja")
    funcs = {
        "numero_correlativo_matricula": registro_matricula.numero_correlativo_matricula,
        "run": registro_matricula.run,
        "ipe": registro_matricula.ipe,
        "primer_nombre_estudiante": registro_matricula.primer_nombre_estudiante,
        "segundo_nombre_estudiante": registro_matricula.segundo_nombre_estudiante,
        "apellido_paterno_estudiante": registro_matricula.apellido_paterno_estudiante,
        "apellido_materno_estudiante": registro_matricula.apellido_materno_estudiante,
        "sexo": registro_matricula.sexo,
        "fecha_nacimiento": registro_matricula.fecha_nacimiento,
        "nivel": registro_matricula.nivel,
        "curso": registro_matricula.curso,
        "local_escolar": registro_matricula.local_escolar,
        "fecha_matricula": registro_matricula.fecha_matricula,
        "fecha_retiro_estudiante": registro_matricula.fecha_retiro_estudiante,
        "motivo_retiro_estudiante": registro_matricula.motivo_retiro_estudiante,
        "domicilio_estudiante": registro_matricula.domicilio_estudiante,
        "observaciones": registro_matricula.observaciones,
        "domicilio_padre": registro_matricula.domicilio_padre,
        "telefono_padre": registro_matricula.telefono_padre,
        "email_padre": registro_matricula.email_padre,
        "domicilio_madre": registro_matricula.domicilio_madre,
        "telefono_madre": registro_matricula.telefono_madre,
        "email_madre": registro_matricula.email_madre,
        "presedencia": registro_matricula.presedencia,
        "nivel_educacional_padre": registro_matricula.nivel_educacional_padre,
        "nivel_educacional_madre": registro_matricula.nivel_educacional_madre,
        "persona_con_quien_vive": registro_matricula.persona_con_quien_vive,
        "tipo_estudiante": registro_matricula.tipo_estudiante,
        "doc_pais_origen": registro_matricula.doc_pais_origen,
        "tipo_matricula": registro_matricula.tipo_matricula,
        "numero_fecha_resolucion": registro_matricula.numero_fecha_resolucion,
        "rbd": registro_matricula.rbd,
        "nombre_establecimiento": registro_matricula.nombre_establecimiento,
        "modalidad": registro_matricula.modalidad,
        "anio_escolar": registro_matricula.anio_escolar,
        "primer_nombre_profesor_jefe": registro_matricula.primer_nombre_profesor_jefe,
        "segundo_nombre_profesor_jefe": registro_matricula.segundo_nombre_profesor_jefe,
        "apellido_paterno_profesor_jefe": registro_matricula.apellido_paterno_profesor_jefe,
        "apellido_materno_profesor_jefe": registro_matricula.apellido_materno_profesor_jefe,
        "domicilio_estudiante_comuna": registro_matricula.domicilio_estudiante_comuna,
        "primer_nombre_apoderado_tutor": registro_matricula.primer_nombre_apoderado_tutor,
        "segundo_nombre_apoderado_tutor": registro_matricula.segundo_nombre_apoderado_tutor,
        "apellido_paterno_apoderado_tutor": registro_matricula.apellido_paterno_apoderado_tutor,
        "apellido_materno_apoderado_tutor": registro_matricula.apellido_materno_apoderado_tutor,
        "telefono_apoderado_tutor": registro_matricula.telefono_apoderado_tutor,
        "email_apoderado_tutor": registro_matricula.email_apoderado_tutor,
        "datos_biologicos_salud_estudiante": registro_matricula.datos_biologicos_salud_estudiante,
        "n_resolucion_fecha_estudiante": registro_matricula.n_resolucion_fecha_estudiante,
        "intercambio_estudiante": registro_matricula.intercambio_estudiante,
        "otro_dato_interes_estudiante": registro_matricula.otro_dato_interes_estudiante,
        "modalidad_dual": registro_matricula.modalidad_dual,
        "nombre_empresa": registro_matricula.nombre_empresa,
        "direccion_empresa": registro_matricula.direccion_empresa,
        "telefono_empresa": registro_matricula.telefono_empresa,
        "comuna_empresa": registro_matricula.comuna_empresa,
        "email_empresa": registro_matricula.email_empresa,
        "primer_nombre_tutor": registro_matricula.primer_nombre_tutor,
        "segundo_nombre_tutor": registro_matricula.segundo_nombre_tutor,
        "apellido_paterno_tutor": registro_matricula.apellido_paterno_tutor,
        "apellido_materno_tutor": registro_matricula.apellido_materno_tutor,
        "telefono_tutor": registro_matricula.telefono_tutor,
        "correo_tutor": registro_matricula.correo_tutor,
        "rut_tutor": registro_matricula.rut_tutor,
        "nacionalidad": registro_matricula.nacionalidad,
        "etnia": registro_matricula.etnia
    }
    print("hola")
    f = funcs[request.GET.get("item", "run")]
    print(f)
    objs, head = f.__call__()  # registro_matricula.numero_correlativo_de_matricula()
    return render(request, "Libro/table_from_sql.html", {"rows": objs, "table_head": head})
