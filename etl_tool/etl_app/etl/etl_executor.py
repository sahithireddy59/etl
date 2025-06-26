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
import pandas as pd
from sqlalchemy import create_engine

def table_exists(cur, table_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def apply_pipeline(df, pipeline_nodes, pipeline_edges):
    # Process nodes in the order: input -> filter -> expression -> rollup -> output
    node_types = ['input', 'filter', 'expression', 'rollup', 'output']
    ordered_nodes = []
    for t in node_types:
        ordered_nodes.extend([n for n in pipeline_nodes if n.get('data', {}).get('type') == t])
    for node in ordered_nodes:
        if node.get('data', {}).get('type') == 'expression':
            for expr in node['data'].get('expressions', []):
                col = expr['column']
                expression = expr['expression']
                # Handle upper(name) and lower(name)
                if expression.lower().startswith('upper(') and expression.endswith(')'):
                    src_col = expression[6:-1].strip()
                    df[col] = df[src_col].str.upper()
                elif expression.lower().startswith('lower(') and expression.endswith(')'):
                    src_col = expression[6:-1].strip()
                    df[col] = df[src_col].str.lower()
                else:
                    try:
                        df[col] = df.eval(expression)
                    except Exception as e:
                        print(f"Expression error: {e}")
        elif node.get('data', {}).get('type') == 'filter':
            condition = node['data'].get('condition', '')
            if condition:
                try:
                    df = df.query(condition)
                except Exception as e:
                    print(f"Filter error: {e}")
        elif node.get('data', {}).get('type') == 'rollup':
            group_by = node['data'].get('groupBy', [])
            aggs = node['data'].get('aggregations', {})
            if group_by and aggs:
                try:
                    df = df.groupby(group_by).agg(aggs).reset_index()
                    # Enforce only group_by and aggregation columns in output
                    allowed_columns = list(group_by) + list(aggs.keys())
                    df = df[[col for col in allowed_columns if col in df.columns]]
                except Exception as e:
                    print(f"Rollup error: {e}")
            elif group_by and not aggs:
                # No aggregations: just keep unique rows for group_by columns
                try:
                    df = df[group_by].drop_duplicates().reset_index(drop=True)
                except Exception as e:
                    print(f"Rollup (distinct) error: {e}")
    return df

def run_etl_job(job):
    # Load source data (for demo, assume source_table is a CSV file path or DB table)
    rule = job.transformation_rule.strip()
    source_table = job.source_table
    target_table = job.target_table
    try:
        pipeline = json.loads(rule)
        if 'nodes' in pipeline and 'edges' in pipeline:
            # For demo: assume source_table is a CSV file path
            if source_table.endswith('.csv'):
                df = pd.read_csv(source_table)
            else:
                # Load from DB
                engine = create_engine('postgresql://postgres:postgres@host.docker.internal/etl_db')
                df = pd.read_sql(f'SELECT * FROM "{source_table}"', engine)
            df_result = apply_pipeline(df, pipeline['nodes'], pipeline['edges'])
            # Debug: print DataFrame info before writing
            print('--- DataFrame to be written to target table ---')
            print(df_result.head())
            print(df_result.dtypes)
            print(f"Rows to write: {len(df_result)}")
            # Write to DB
            engine = create_engine('postgresql://postgres:postgres@host.docker.internal/etl_db')
            df_result.to_sql(target_table, engine, if_exists='append', index=False)
            print(f"✅ ETL complete: {len(df_result)} rows written to {target_table}")
            return
    except Exception as e:
        print(f"Pipeline parse error or not a pipeline: {e}")
        raise
    # Fallback: old logic
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
    # Prevent fallback if transformation_rule is a pipeline JSON
    if rule.startswith('{') and rule.endswith('}'):
        print("❌ Invalid fallback: transformation_rule is a pipeline JSON, not a SQL expression. Skipping fallback SQL execution.")
        cur.close()
        conn.close()
        return
    # Try to parse as JSON list of expressions
    try:
        exprs = json.loads(rule)
        if isinstance(exprs, list) and len(exprs) > 0 and 'expression' in exprs[0] and 'column' in exprs[0]:
            select_clause = ', '.join([
                f"{e['expression']} AS {e['column']}" for e in exprs
            ])
            columns = ', '.join([e['column'] for e in exprs])
        else:
            select_clause = rule if rule else '*'
            columns = None
    except Exception:
        select_clause = rule if rule else '*'
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
