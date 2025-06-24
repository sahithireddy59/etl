from django import forms
from .models import ETLJob

class ETLJobForm(forms.ModelForm):
    class Meta:
        model = ETLJob
        fields = ['name', 'source_table', 'target_table', 'transformation_rule']
        widgets = {
            'transformation_rule': forms.Textarea(attrs={'rows': 4, 'cols': 80}),
        }
