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
from etl_app.models import ETLNodeStatus
from django.utils import timezone

def table_exists(cur, table_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def apply_pipeline(df, pipeline_nodes, pipeline_edges, job):
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
        elif node.get('data', {}).get('type') == 'joiner':
            left_table_id = node['data'].get('leftTable', '')
            right_table_id = node['data'].get('rightTable', '')
            left_key = node['data'].get('leftKey', '')
            right_key = node['data'].get('rightKey', '')
            join_type = node['data'].get('joinType', 'inner')
            selected_fields = node['data'].get('selectedFields', [])
            field_name_map = node['data'].get('fieldNameMap', {})

            # For demo: map node id to table name (in real app, store mapping in node or pipeline)
            node_id_to_table = {
                '1': 'customers',
                '2': 'orders',
            }
            left_table = node_id_to_table.get(left_table_id, left_table_id)
            right_table = node_id_to_table.get(right_table_id, right_table_id)

            if left_table and right_table:
                try:
                    # Load left and right tables
                    engine = create_engine('postgresql://postgres:postgres@host.docker.internal/release?options=-csearch_path%3Danimal_biome_production')
                    df_left = pd.read_sql(f'SELECT * FROM "{left_table}"', engine)
                    df_right = pd.read_sql(f'SELECT * FROM "{right_table}"', engine)

                    # Validate join keys
                    if join_type != 'cross':
                        if left_key not in df_left.columns or right_key not in df_right.columns:
                            raise Exception(f"Join key missing: {left_key} in left or {right_key} in right table")

                    # Handle cross join
                    if join_type == 'cross':
                        df_left['_tmpkey'] = 1
                        df_right['_tmpkey'] = 1
                        df = pd.merge(df_left, df_right, on='_tmpkey', how='outer', suffixes=('', '_r'))
                        df.drop('_tmpkey', axis=1, inplace=True)
                    else:
                        # Avoid column collisions by prefixing right table columns (except join key)
                        right_cols = [c for c in df_right.columns if c != right_key]
                        for col in right_cols:
                            if col in df_left.columns:
                                df_right.rename(columns={col: f'r_{col}'}, inplace=True)
                        # Update right_key if renamed
                        right_key_renamed = f'r_{right_key}' if right_key in df_left.columns else right_key
                        df = pd.merge(df_left, df_right, how=join_type, left_on=left_key, right_on=right_key_renamed, suffixes=('', '_r'))

                    # Select only the requested fields
                    if selected_fields:
                        df = df[[col for col in selected_fields if col in df.columns]]
                    # Rename columns according to field_name_map
                    if field_name_map:
                        df = df.rename(columns=field_name_map)
                    # else keep all columns
                except Exception as e:
                    print(f"Joiner error: {e}")
        log_node_status(job.id, node['id'], node['data']['type'], 'success')
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
                engine = create_engine('postgresql://postgres:postgres@host.docker.internal/release?options=-csearch_path%3Danimal_biome_production')
                df = pd.read_sql(f'SELECT * FROM "{source_table}"', engine)
            df_result = apply_pipeline(df, pipeline['nodes'], pipeline['edges'], job)
            # Debug: print DataFrame info before writing
            print('--- DataFrame to be written to target table ---')
            print(df_result.head())
            print(df_result.dtypes)
            print(f"Rows to write: {len(df_result)}")
            print("About to write to SQL. DataFrame shape:", df_result.shape)
            if not df_result.empty:
                print("Writing non-empty DataFrame to SQL")
                df_result.to_sql(target_table, engine, if_exists='append', index=False, schema='animal_biome_production')
            else:
                print("DataFrame is empty, creating table structure only")
                df_result.head(0).to_sql(target_table, engine, if_exists='replace', index=False, schema='animal_biome_production')
            print("Write to SQL complete")
            return
    except Exception as e:
        print(f"Pipeline parse error or not a pipeline: {e}")
        raise
    # Fallback: old logic
    conn = psycopg2.connect(
        dbname='release',
        user='postgres',
        password='postgres',
        host=os.environ.get('DB_HOST', 'localhost'),
        port='5432',
        options='-c search_path=animal_biome_production'
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
    import json

    # Parse the transformation_rule string to get nodes and edges
    try:
        tr = job.transformation_rule
        if isinstance(tr, str):
            tr = json.loads(tr)
        nodes = tr.get("nodes", [])
        edges = tr.get("edges", [])
        pipeline_data = {"nodes": nodes, "edges": edges}
    except Exception as e:
        print(f"Error parsing transformation_rule: {e}")
        pipeline_data = {"nodes": [], "edges": []}

    airflow_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../shared/etl_jobs.json"))
    os.makedirs(os.path.dirname(airflow_file), exist_ok=True)

    with open(airflow_file, "w") as f:
        json.dump(pipeline_data, f, indent=2)

def log_node_status(job_id, node_id, node_type, status, message=''):
    ETLNodeStatus.objects.create(
        job_id=job_id,
        node_id=node_id,
        node_type=node_type,
        status=status,
        message=message,
        timestamp=timezone.now()
    )
