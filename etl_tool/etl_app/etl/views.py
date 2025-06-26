from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.db import connection

@require_GET
def table_columns(request):
    table = request.GET.get('table')
    if not table:
        return JsonResponse({'error': 'Missing table parameter'}, status=400)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT * FROM "{table}" LIMIT 0')
            columns = [col[0] for col in cursor.description]
        return JsonResponse({'columns': columns})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500) 