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

class ETLNodeStatus(models.Model):
    job_id = models.IntegerField()
    node_id = models.CharField(max_length=64)
    node_type = models.CharField(max_length=32)
    status = models.CharField(max_length=16)  # e.g., 'success', 'failure', 'running'
    message = models.TextField(blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Job {self.job_id} Node {self.node_id} [{self.status}]"
