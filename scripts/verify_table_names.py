#!/usr/bin/env python3

"""
Verify Table Names Script
=========================
This script verifies that table names match between catalog schema and Glue jobs.
"""

import json
import os
import re
from pathlib import Path

def load_catalog_schemas():
    schema_file = Path(__file__).parent.parent / 'docs' / 'catalog_table_schemas.json'
    
    try:
        with open(schema_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Schema file not found: {schema_file}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing schema file: {str(e)}")
        return None

def extract_table_names_from_glue_job(file_path):
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        table_names = set()
        
        # Look for table references in various patterns
        patterns = [
            r"CATALOG_TABLES\s*=\s*{([^}]+)}",
            r"save_to_iceberg_table\([^,]+,\s*['\"]([^'\"]+)['\"]",
            r"\.saveAsTable\(['\"]([^'\"]+)['\"]",
            r"CREATE TABLE[^(]+([a-zA-Z_][a-zA-Z0-9_]*)",
            r"INSERT INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                if isinstance(match, tuple):
                    table_names.update(match)
                else:
                    table_names.add(match)
        
        # Clean up table names
        cleaned_names = set()
        for name in table_names:
            cleaned = name.strip().strip('"\'').strip()
            if cleaned and not cleaned.startswith('${') and len(cleaned) > 2:
                cleaned_names.add(cleaned)
        
        return cleaned_names
        
    except Exception as e:
        print(f"❌ Error reading {file_path}: {str(e)}")
        return set()

def main():
    print("Table Names Verification")
    print("=" * 50)
    
    # Load catalog schemas
    catalog_schemas = load_catalog_schemas()
    if not catalog_schemas:
        return
    
    catalog_tables = set(catalog_schemas['tables'].keys())
    print(f"Catalog tables ({len(catalog_tables)}):")
    for table in sorted(catalog_tables):
        print(f"  • {table}")
    
    # Check Glue job files
    glue_jobs_dir = Path(__file__).parent.parent / 'glue_jobs'
    glue_files = list(glue_jobs_dir.glob('*.py'))
    
    print(f"\nGlue job files ({len(glue_files)}):")
    all_job_tables = set()
    
    for glue_file in glue_files:
        if glue_file.name.startswith('0') and glue_file.name.endswith('.py'):
            print(f"\n🔍 Checking: {glue_file.name}")
            
            job_tables = extract_table_names_from_glue_job(glue_file)
            all_job_tables.update(job_tables)
            
            if job_tables:
                for table in sorted(job_tables):
                    status = "✅" if table in catalog_tables else "❌"
                    print(f"    {status} {table}")
            else:
                print("    No table names found")
    
    # Summary
    print(f"\n{'='*50}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*50}")
    
    # Tables in catalog but not in jobs
    missing_in_jobs = catalog_tables - all_job_tables
    if missing_in_jobs:
        print(f"\n❌ Tables in catalog but not referenced in jobs ({len(missing_in_jobs)}):")
        for table in sorted(missing_in_jobs):
            print(f"  • {table}")
    
    # Tables in jobs but not in catalog
    missing_in_catalog = all_job_tables - catalog_tables
    if missing_in_catalog:
        print(f"\n❌ Tables referenced in jobs but not in catalog ({len(missing_in_catalog)}):")
        for table in sorted(missing_in_catalog):
            print(f"  • {table}")
    
    # Matching tables
    matching_tables = catalog_tables & all_job_tables
    if matching_tables:
        print(f"\n✅ Matching tables ({len(matching_tables)}):")
        for table in sorted(matching_tables):
            print(f"  • {table}")
    
    # Overall status
    if not missing_in_jobs and not missing_in_catalog:
        print(f"\n✅ All table names are consistent!")
    else:
        print(f"\n❌ Table name inconsistencies found.")
        print("   Please update either the catalog schema or Glue job references.")

if __name__ == "__main__":
    main() 