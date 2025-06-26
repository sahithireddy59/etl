import sys
import argparse
from datetime import date
from django.core.management.base import BaseCommand
from django.db import connections, transaction

class Command(BaseCommand):
    help = 'Generic SCD Type 2 ETL: Compare source and target tables, update/inserts as needed.'

    def add_arguments(self, parser):
        parser.add_argument('--source-table', required=True, help='Source table name')
        parser.add_argument('--target-table', required=True, help='Target (dimension) table name')
        parser.add_argument('--business-key', required=True, help='Business key column(s), comma-separated if multiple')
        parser.add_argument('--compare-columns', required=True, help='Columns to compare for changes, comma-separated')
        parser.add_argument('--database', default='default', help='Django DB alias (default: default)')

    def handle(self, *args, **options):
        source_table = options['source_table']
        target_table = options['target_table']
        business_keys = [k.strip() for k in options['business_key'].split(',')]
        compare_columns = [c.strip() for c in options['compare_columns'].split(',')]
        db_alias = options['database']

        conn = connections[db_alias]
        cursor = conn.cursor()

        # Fetch all source records
        cursor.execute(f'SELECT * FROM {source_table}')
        source_rows = cursor.fetchall()
        source_columns = [col[0] for col in cursor.description]

        # For each source record, process SCD2 logic
        inserted, updated, skipped = 0, 0, 0
        for row in source_rows:
            record = dict(zip(source_columns, row))
            # Build WHERE clause for business key
            where_clause = ' AND '.join([f"{k} = %s" for k in business_keys]) + " AND is_active = 'Y'"
            where_values = [record[k] for k in business_keys]
            cursor.execute(
                f'SELECT * FROM {target_table} WHERE {where_clause}',
                where_values
            )
            target_row = cursor.fetchone()
            if not target_row:
                # Insert new record
                insert_columns = business_keys + compare_columns + ['is_active', 'start_date', 'end_date']
                insert_values = [record[k] for k in business_keys + compare_columns] + ['Y', date.today(), None]
                placeholders = ','.join(['%s'] * len(insert_columns))
                cursor.execute(
                    f'INSERT INTO {target_table} ({", ".join(insert_columns)}) VALUES ({placeholders})',
                    insert_values
                )
                inserted += 1
                self.stdout.write(self.style.SUCCESS(f"Inserted: {where_values}"))
            else:
                # Compare columns
                target_desc = [col[0] for col in cursor.description]
                target_dict = dict(zip(target_desc, target_row))
                changed = any(record[c] != target_dict.get(c) for c in compare_columns)
                if changed:
                    # Mark old as inactive
                    update_sql = f"UPDATE {target_table} SET is_active = 'N', end_date = %s WHERE {where_clause}"
                    cursor.execute(update_sql, [date.today()] + where_values)
                    # Insert new record
                    insert_columns = business_keys + compare_columns + ['is_active', 'start_date', 'end_date']
                    insert_values = [record[k] for k in business_keys + compare_columns] + ['Y', date.today(), None]
                    placeholders = ','.join(['%s'] * len(insert_columns))
                    cursor.execute(
                        f'INSERT INTO {target_table} ({", ".join(insert_columns)}) VALUES ({placeholders})',
                        insert_values
                    )
                    updated += 1
                    self.stdout.write(self.style.WARNING(f"Updated: {where_values}"))
                else:
                    skipped += 1
                    self.stdout.write(self.style.NOTICE(f"Skipped (no change): {where_values}"))
        conn.commit()
        self.stdout.write(self.style.SUCCESS(f"Done. Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}")) 