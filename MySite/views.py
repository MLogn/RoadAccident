from django.shortcuts import render
from django.views.generic import TemplateView
from django.contrib.auth.models import User
from .db_functions import DbFunctions


class TestView(TemplateView):
    template_name = 'MySite/about.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['user'] = self.request.user
        return context

def index(request):
    return render(request, 'MySite/main.html')

def person(request):
    return render(request, 'MySite/person.html')

def stat(request):
    return render(request, 'MySite/stat.html')


from django.http import HttpResponseRedirect
from django.http import HttpResponseNotFound
from .models import Person


# получение данных из бд
def change(request):
    people = Person.objects.all()
    return render(request, "MySite/change.html", {"people": people})


# сохранение данных в бд
def create(request):
    if request.method == "POST":
        db = DbFunctions()
        user_info = {}
        user_id = request.POST.get("user_id")
        user_info['user_id'] = user_id
        age = request.POST.get("age")
        print("age", age)
        user_info['age'] = age
        veh_type = request.POST.get("veh_type")
        user_info['veh_type'] = veh_type
        gender = request.POST.get("gender")
        user_info['gender'] = gender
        vehicle_age = request.POST.get("vehicle_age")
        user_info['vehicle_age'] = vehicle_age
        date_from = request.POST.get("date_from")
        user_info['date_from'] = date_from
        date_to = request.POST.get("date_to")
        user_info['date_to'] = date_to
        db.add_user_info(user_id, user_info)

    return HttpResponseRedirect("/MySite/change.html")


# изменение данных в бд
def edit(request, id):
    try:
        person = Person.objects.get(id=id)

        if request.method == "POST":
            person.user_id = request.POST.get("user_id")
            person.age = request.POST.get("age")
            person.save()
            return HttpResponseRedirect("/MySite/change.html")
        else:
            return render(request, "MySite/edit.html", {"person": person})
    except Person.DoesNotExist:
        return HttpResponseNotFound("<h2>Person not found</h2>")


# удаление данных из бд
def delete(request, id):
    try:
        person = Person.objects.get(id=id)
        person.delete()
        return HttpResponseRedirect("/MySite/change.html")
    except Person.DoesNotExist:
        return HttpResponseNotFound("<h2>Person not found</h2>")