from django.db import models

class ETLJob(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=100)
    source_table = models.CharField(max_length=100)
    target_table = models.CharField(max_length=100)
    transformation_rule = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
