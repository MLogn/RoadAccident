from django.contrib.auth.models import User
from django.db import models

class person(models.Model):
    title = models.CharField(max_length=100)
    text = models.TextField()
    #user = models.ForeignKey(User)