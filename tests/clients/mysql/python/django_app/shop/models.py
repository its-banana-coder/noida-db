from django.db import models


class Author(models.Model):
    name = models.CharField(max_length=50, unique=True)
    created = models.DateTimeField(auto_now_add=True)


class Book(models.Model):
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="books")
    title = models.CharField(max_length=100)
    price = models.DecimalField(max_digits=8, decimal_places=2)
    tags = models.JSONField(default=list)
    published = models.BooleanField(default=False)

    class Meta:
        indexes = [models.Index(fields=["title"])]
