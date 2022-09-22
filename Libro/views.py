from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from . import merge_database
from django.db import connection

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


def show_students(request):
    with connection.cursor() as cursor:
        objs = cursor.execute("SELECT * FROM Person")
        # print([x for x in objs])
        return render(request, "Libro/table_from_sql.html", {"rows": [x for x in objs]})
