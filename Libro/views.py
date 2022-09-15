from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from . import merge_database


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
    else:
        return HttpResponse("No hay implementación")
