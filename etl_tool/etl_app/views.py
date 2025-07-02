from django.shortcuts import render, redirect
from .forms import ETLJobForm
from .models import ETLJob, ETLNodeStatus
from .etl.etl_executor import run_etl_job, save_job_to_airflow
from .etl.airflow_trigger import trigger_airflow_dag_with_job  # ✅ Add this
from django.http import JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
from django.views.decorators.http import require_http_methods, require_GET, require_POST
import os
import subprocess
import requests

def home(request):
    jobs = ETLJob.objects.all()
    return render(request, 'etl_app/index.html', {'jobs': jobs})

@csrf_exempt
def create_job(request):
    if request.method == 'POST':
        if request.content_type == 'application/json':
            data = json.loads(request.body)
            nodes = data.get('nodes', [])
            edges = data.get('edges', [])
            # Improved job name logic (fix: check node['data']['type'])
            job_name = data.get('name')
            if not job_name:
                has_filter = any(node.get('data', {}).get('type') == 'filter' for node in nodes)
                has_expression = any(node.get('data', {}).get('type') == 'expression' for node in nodes)
                if has_filter and has_expression:
                    job_name = "Filter + Expression Load"
                elif has_filter:
                    job_name = "Filter Load"
                elif has_expression:
                    job_name = "Expression Load"
                else:
                    job_name = "One-to-One Load"
            source_table = ''
            target_table = ''
            for node in nodes:
                node_type = node.get('data', {}).get('type', node.get('type'))
                if node_type == 'input':
                    source_table = node.get('data', {}).get('label', '')
                elif node_type == 'output':
                    target_table = node.get('data', {}).get('label', '')
            import json as _json
            # Always save as JSON, even if nodes/edges are empty
            transformation_rule_json = _json.dumps({'nodes': nodes, 'edges': edges})
            job = ETLJob.objects.create(
                name=job_name,
                source_table=source_table,
                target_table=target_table,
                transformation_rule=transformation_rule_json
            )
            print("✅ Job saved in Django. Now writing to Airflow.")
            save_job_to_airflow(job)
            trigger_airflow_dag_with_job(job)
            print("✅ Airflow trigger called")
            return JsonResponse({'status': 'success'})
        else:
            # Existing form handling for HTML form POSTs
            form = ETLJobForm(request.POST)
            if form.is_valid():
                job = form.save()
                print("✅ Job saved in Django. Now writing to Airflow.")
                save_job_to_airflow(job)
                trigger_airflow_dag_with_job(job)
                print("✅ Airflow trigger called")
                return redirect('home')
    else:
        form = ETLJobForm()
    return render(request, 'etl_app/create_job.html', {'form': form})

def job_list(request):
    jobs = ETLJob.objects.all().order_by('-id')
    data = [
        {
            'id': job.id,
            'source_table': job.source_table,
            'target_table': job.target_table,
            'transformation_rule': job.transformation_rule,
            'created_at': job.created_at.strftime('%Y-%m-%d %H:%M:%S') if hasattr(job, 'created_at') else '',
            'status': getattr(job, 'status', 'unknown'),
        }
        for job in jobs
    ]
    return JsonResponse({'jobs': data})

def table_data(request):
    table = request.GET.get('table')
    if not table:
        return JsonResponse({'error': 'No table specified'}, status=400)
    with connection.cursor() as cursor:
        try:
            cursor.execute(f'SELECT * FROM "{table}" LIMIT 100')
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    return JsonResponse({'columns': columns, 'rows': rows})

@csrf_exempt
@require_http_methods(["DELETE"])
def delete_job(request, job_id):
    try:
        job = ETLJob.objects.get(id=job_id)
        job.delete()
        return JsonResponse({'status': 'deleted'})
    except ETLJob.DoesNotExist:
        return JsonResponse({'error': 'Job not found'}, status=404)

@require_GET
def node_status(request):
    job_id = request.GET.get('job_id')
    if not job_id:
        return JsonResponse({'error': 'Missing job_id'}, status=400)
    statuses = ETLNodeStatus.objects.filter(job_id=job_id).order_by('timestamp')
    data = [
        {
            'node_id': s.node_id,
            'node_type': s.node_type,
            'status': s.status,
            'message': s.message,
            'timestamp': s.timestamp.isoformat(),
        }
        for s in statuses
    ]
    return JsonResponse({'statuses': data})

@csrf_exempt
@require_POST
def save_pipeline(request):
    try:
        data = json.loads(request.body)
        # Extract nodes/edges from transformation_rule if present
        if "transformation_rule" in data:
            tr = data["transformation_rule"]
            if isinstance(tr, str):
                tr = json.loads(tr)
            nodes = tr.get("nodes", [])
            edges = tr.get("edges", [])
            pipeline_data = {"nodes": nodes, "edges": edges}
        else:
            pipeline_data = {"nodes": data.get("nodes", []), "edges": data.get("edges", [])}
        # Write only nodes/edges to etl_jobs.json
        with open("shared/etl_jobs.json", "w") as f:
            json.dump(pipeline_data, f)
        return JsonResponse({"status": "success"})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def update_pipeline(request):
    try:
        data = json.loads(request.body)
        if "transformation_rule" in data:
            tr = data["transformation_rule"]
            if isinstance(tr, str):
                tr = json.loads(tr)
            nodes = tr.get("nodes", [])
            edges = tr.get("edges", [])
            pipeline_data = {"nodes": nodes, "edges": edges}
        else:
            pipeline_data = {"nodes": data.get("nodes", []), "edges": data.get("edges", [])}
        with open("shared/etl_jobs.json", "w") as f:
            json.dump(pipeline_data, f)
        return JsonResponse({"status": "success"})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
@require_http_methods(["DELETE"])
def delete_pipeline(request):
    try:
        data = json.loads(request.body)
        dag_id = data.get("dag_id")
        if not dag_id:
            return JsonResponse({"status": "error", "message": "dag_id required"}, status=400)
        pipeline_json_path = f"shared/{dag_id}.json"
        dag_file_path = f"dags/{dag_id}.py"
        # Delete files if they exist
        if os.path.exists(pipeline_json_path):
            os.remove(pipeline_json_path)
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
        return JsonResponse({"status": "success", "dag_id": dag_id})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
