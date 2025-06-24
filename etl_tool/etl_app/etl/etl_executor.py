def generate_sql(source_table, target_table, transformation_rule):
    """
    Generates SQL based on the transformation_rule:
    - FORMAT: "FILTER: column='value'"
    - FORMAT: "GROUPBY: column"
    - FORMAT: "JOIN: other_table ON table.id = other_table.table_id"
    - FORMAT: "UPPER(name)" or any SQL expression for transformation
    """
    base_query = f"SELECT * FROM {source_table}"

    rule = transformation_rule.strip()
    if rule.upper().startswith("FILTER:"):
        condition = rule[7:].strip()
        sql = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table} WHERE {condition}"
    elif rule.upper().startswith("GROUPBY:"):
        group_column = rule[8:].strip()
        sql = f"""CREATE TABLE {target_table} AS
                 SELECT {group_column}, COUNT(*) AS count
                 FROM {source_table}
                 GROUP BY {group_column}"""
    elif rule.upper().startswith("JOIN:"):
        join_clause = rule[5:].strip()
        sql = f"""CREATE TABLE {target_table} AS
                 SELECT a.*, b.*
                 FROM {source_table} a
                 JOIN {join_clause}"""
    elif rule != "":
        # Handle function names
        if rule.lower() in ['upper', 'lower']:
            sql = f"CREATE TABLE {target_table} AS SELECT id, {rule.upper()}(name) AS name, age FROM {source_table}"
        elif rule.isidentifier():
            sql = f"CREATE TABLE {target_table} AS SELECT id, {rule}(name) AS name, age FROM {source_table}"
        else:
            # Assume transformation_rule is a full SQL expression for the 'name' column
            sql = f"CREATE TABLE {target_table} AS SELECT *, {rule} AS name_transformed FROM {source_table}"
    else:
        # Default fallback
        sql = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"

    return sql


import json
import os
import psycopg2

def table_exists(cur, table_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def run_etl_job(job):
    conn = psycopg2.connect(
        dbname='etl_db',
        user='postgres',
        password='postgres',
        host='host.docker.internal',
        port='5432'
    )
    cur = conn.cursor()

    rule = job.transformation_rule.strip()
    source_table = job.source_table
    target_table = job.target_table
    # Try to parse as JSON list of expressions
    try:
        exprs = json.loads(rule)
        if isinstance(exprs, list) and len(exprs) > 0 and 'expression' in exprs[0] and 'column' in exprs[0]:
            select_clause = ', '.join([
                f"{e['expression']} AS {e['column']}" for e in exprs
            ])
            columns = ', '.join([e['column'] for e in exprs])
        else:
            select_clause = rule
            columns = None
    except Exception:
        select_clause = rule
        columns = None

    if not table_exists(cur, target_table):
        # Table does not exist: create it
        create_sql = f"CREATE TABLE {target_table} AS SELECT {select_clause} FROM {source_table};"
        cur.execute(create_sql)
    else:
        # Table exists: insert new data
        if columns:
            insert_sql = f"INSERT INTO {target_table} ({columns}) SELECT {select_clause} FROM {source_table};"
        else:
            insert_sql = f"INSERT INTO {target_table} SELECT {select_clause} FROM {source_table};"
        cur.execute(insert_sql)
    conn.commit()
    cur.close()
    conn.close()

def save_job_to_airflow(job):
    job_dict = {
        "name": job.name,
        "source_table": job.source_table,
        "target_table": job.target_table,
        "transformation_rule": job.transformation_rule
    }

    airflow_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../shared/etl_jobs.json"))
    os.makedirs(os.path.dirname(airflow_file), exist_ok=True)

    with open(airflow_file, "w") as f:
        json.dump(job_dict, f)
