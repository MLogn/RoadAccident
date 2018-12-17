from django.db import models
from django.contrib.auth.models import User
from django.conf import settings

class Account(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    bio = models.CharField(max_length=100, null=True, blank=True)
    bio1 = models.CharField(max_length=100, null=True, blank=True)
    bio2 = models.CharField(max_length=100, null=True, blank=True)
    bio3 = models.CharField(max_length=100, null=True, blank=True)
    bio4 = models.CharField(max_length=100, null=True, blank=True)
    bio5 = models.CharField(max_length=100, null=True, blank=True)
    bio6 = models.CharField(max_length=100, null=True, blank=True)

class Person(models.Model):
    user_id = models.CharField(max_length=20, default='')
    veh_type = models.CharField(max_length=20)
    gender = models.CharField(max_length=20)
    date_from = models.CharField(max_length=20, default='')
    date_to = models.CharField(max_length=20, default='')
    age = models.CharField(max_length=20)
    vehicle_age = models.CharField(max_length=20)