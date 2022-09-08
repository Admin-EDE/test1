from django.shortcuts import render
from django.http import HttpResponse, JsonResponse


# Create your views here.
def login(request):
    pass


def upload_database(request):
    if request.method == "GET":
        return render(request, "Libro/upload_database.html")
    elif request.method == "POST":
        pass
    else:
        return HttpResponse("No hay implementación")