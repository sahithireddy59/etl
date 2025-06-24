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

import datetime

def run_etl_job(job):
    # SCD Type 2 specific parameters from job object (passed via Airflow conf)
    scd_type = getattr(job, 'scd_type', None)
    business_key_column = getattr(job, 'business_key_column', None)
    tracked_attribute_columns_json = getattr(job, 'tracked_attribute_columns', '[]')

    # Default SCD column names
    scd_start_date_column = getattr(job, 'scd_start_date_column', 'start_date')
    scd_end_date_column = getattr(job, 'scd_end_date_column', 'end_date')
    scd_is_active_column = getattr(job, 'scd_is_active_column', 'is_active')

    try:
        tracked_attribute_columns = json.loads(tracked_attribute_columns_json)
        if not isinstance(tracked_attribute_columns, list):
            tracked_attribute_columns = [] # Default to empty if not a list
    except json.JSONDecodeError:
        print(f"Warning: Could not parse tracked_attribute_columns JSON: {tracked_attribute_columns_json}. Defaulting to empty list.")
        tracked_attribute_columns = []

    conn = psycopg2.connect(
        dbname='etl_db',
        user='postgres',
        password='postgres',
        host='host.docker.internal', # Assumes Airflow worker can reach this
        port='5432'
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) # Use DictCursor for easy column access by name

    source_table = job.source_table
    target_table = job.target_table
    transformation_rule = job.transformation_rule.strip()

    # --- Determine source columns and select clause from transformation_rule ---
    source_columns_for_select = "*" # Default
    final_source_column_names = [] # Names of columns after transformation

    try:
        # This part tries to parse the transformation_rule if it's a JSON list of expressions
        # e.g., "[{\"column\": \"name_upper\", \"expression\": \"UPPER(name)\"}, ...]"
        transformed_expressions = json.loads(transformation_rule)
        if isinstance(transformed_expressions, list) and transformed_expressions and 'expression' in transformed_expressions[0] and 'column' in transformed_expressions[0]:
            source_columns_for_select = ', '.join([
                f"{e['expression']} AS {e['column']}" for e in transformed_expressions
            ])
            final_source_column_names = [e['column'] for e in transformed_expressions]
        else: # If it's JSON but not the expected structure, or not a list
            # Fallback: if transformation_rule is a simple string (e.g. "COL1, COL2") or a single complex expression
            source_columns_for_select = transformation_rule
            # We can't easily get final_source_column_names here without parsing SQL, so we'll fetch them later
    except json.JSONDecodeError:
        # Fallback: if transformation_rule is not JSON, assume it's a list of columns or expressions
        source_columns_for_select = transformation_rule if transformation_rule else "*"
        # We'll fetch column names after the query if it's "*"

    # --- SCD Type 2 Logic ---
    if scd_type == "2" and business_key_column and tracked_attribute_columns:
        print(f"Starting SCD Type 2 processing for target table: {target_table}")
        print(f"Business Key: {business_key_column}")
        print(f"Tracked Attributes: {tracked_attribute_columns}")

        if not table_exists(cur, target_table):
            # For SCD2, the table should ideally be pre-created with the correct structure.
            # Creating it on the fly here based on source could miss SCD specific columns like sk, start_date, end_date, is_active
            # For now, we'll log an error. A more robust solution would be to create it with SCD fields.
            print(f"ERROR: Target table {target_table} does not exist. SCD Type 2 requires a pre-existing dimension table with SCD columns.")
            cur.close()
            conn.close()
            return

        # Fetch all source data after transformation
        # We need all columns to be able to insert them into the target.
        source_query = f"SELECT {source_columns_for_select} FROM {source_table}"
        print(f"Executing source query: {source_query}")
        cur.execute(source_query)
        source_records = cur.fetchall()

        if not source_records:
            print("No records found in source table after transformation.")
            cur.close()
            conn.close()
            return

        # Get actual column names from the source query result if not determined from JSON rule
        if not final_source_column_names:
            final_source_column_names = [desc[0] for desc in cur.description]

        print(f"Source columns (after transformation): {final_source_column_names}")

        # Ensure business key and tracked attributes are in the selected source columns
        if business_key_column not in final_source_column_names:
            raise ValueError(f"Business key column '{business_key_column}' not found in transformed source columns: {final_source_column_names}")
        for col in tracked_attribute_columns:
            if col not in final_source_column_names:
                raise ValueError(f"Tracked attribute column '{col}' not found in transformed source columns: {final_source_column_names}")

        # All columns to be inserted/updated in the target table (excluding surrogate key)
        target_value_columns = [col for col in final_source_column_names if col != business_key_column] # Exclude BK if it's part of general attributes for insert

        insert_cols_list = final_source_column_names + [scd_start_date_column, scd_end_date_column, scd_is_active_column]
        insert_cols_sql = ", ".join(insert_cols_list)

        placeholders = ", ".join(["%s"] * len(insert_cols_list))

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        for src_row in source_records:
            src_dict = dict(src_row) # Convert psycopg2.extras.DictRow to a plain dict
            business_key_value = src_dict[business_key_column]

            # Lookup active record in target dimension
            lookup_sql = f"""
                SELECT * FROM {target_table}
                WHERE {business_key_column} = %s AND {scd_is_active_column} = TRUE
            """
            cur.execute(lookup_sql, (business_key_value,))
            active_target_row = cur.fetchone()

            if active_target_row:
                active_target_dict = dict(active_target_row)
                # Compare tracked attributes
                changes_found = False
                for attr in tracked_attribute_columns:
                    # Handle None values carefully in comparison
                    src_attr_val = src_dict.get(attr)
                    tgt_attr_val = active_target_dict.get(attr)
                    if src_attr_val != tgt_attr_val:
                        print(f"Change detected for BK {business_key_value}: Attribute '{attr}' changed from '{tgt_attr_val}' to '{src_attr_val}'")
                        changes_found = True
                        break

                if changes_found:
                    # 1. Update old record: set is_active = FALSE, end_date = yesterday
                    update_sql = f"""
                        UPDATE {target_table}
                        SET {scd_is_active_column} = FALSE, {scd_end_date_column} = %s
                        WHERE {scd_is_active_column} = TRUE AND {business_key_column} = %s
                              AND {scd_start_date_column} = %s -- Ensure we update the correct version using start_date
                    """
                    # Using active_target_dict[scd_start_date_column] for precision
                    cur.execute(update_sql, (yesterday, business_key_value, active_target_dict[scd_start_date_column]))
                    print(f"Updated old record for BK {business_key_value} (Start Date: {active_target_dict[scd_start_date_column]}) to inactive.")

                    # 2. Insert new record
                    new_row_values = [src_dict.get(col_name) for col_name in final_source_column_names] + [today, None, True]
                    insert_sql = f"INSERT INTO {target_table} ({insert_cols_sql}) VALUES ({placeholders})"
                    cur.execute(insert_sql, new_row_values)
                    print(f"Inserted new active record for BK {business_key_value}.")
                else:
                    print(f"No changes detected for BK {business_key_value}. Skipping.")
            else:
                # No active record found, insert as new
                new_row_values = [src_dict.get(col_name) for col_name in final_source_column_names] + [today, None, True]
                insert_sql = f"INSERT INTO {target_table} ({insert_cols_sql}) VALUES ({placeholders})"
                cur.execute(insert_sql, new_row_values)
                print(f"No active record found for BK {business_key_value}. Inserted as new.")

    else: # Original logic if not SCD Type 2
        print(f"Running standard ETL for target table: {target_table}")
        # Determine select_clause and columns for insertion (original logic)
        select_clause_orig = "*"
        insert_target_columns_orig = None # For "INSERT INTO target (cols) ..."

        try:
            exprs = json.loads(transformation_rule) # Check if transformation_rule is JSON
            if isinstance(exprs, list) and len(exprs) > 0 and 'expression' in exprs[0] and 'column' in exprs[0]:
                select_clause_orig = ', '.join([f"{e['expression']} AS {e['column']}" for e in exprs])
                insert_target_columns_orig = ', '.join([e['column'] for e in exprs])
            else: # JSON but not the list of dicts format
                select_clause_orig = transformation_rule if transformation_rule else "*"
        except json.JSONDecodeError: # Not JSON
            select_clause_orig = transformation_rule if transformation_rule else "*"

        if not table_exists(cur, target_table):
            # Table does not exist: create it
            # Note: This simple CREATE TABLE AS might not get all column types correct from complex expressions.
            create_sql = f"CREATE TABLE {target_table} AS SELECT {select_clause_orig} FROM {source_table};"
            print(f"Executing: {create_sql}")
            cur.execute(create_sql)
        else:
            # Table exists: insert new data
            if insert_target_columns_orig:
                insert_sql = f"INSERT INTO {target_table} ({insert_target_columns_orig}) SELECT {select_clause_orig} FROM {source_table};"
            else:
                # If specific columns for insert aren't derived, try to insert into table selecting from source.
                # This assumes the columns from SELECT match the target table structure or target table allows selecting fewer columns.
                insert_sql = f"INSERT INTO {target_table} SELECT {select_clause_orig} FROM {source_table};"
            print(f"Executing: {insert_sql}")
            cur.execute(insert_sql)

    conn.commit()
    cur.close()
    conn.close()
    print("ETL job finished.")

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
