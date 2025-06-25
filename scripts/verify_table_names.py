#!/usr/bin/env python3

"""
Verify Table Names Consistency
==============================

This script verifies that table names defined in Glue scripts
match the catalog table setup scripts.
"""

import re
import sys
from pathlib import Path

def extract_table_names_from_glue_script():
    """Extract table names from the Glue script"""
    glue_script_path = Path("glue_jobs/01_raw_data_transformation.py")
    
    if not glue_script_path.exists():
        print(f"❌ Glue script not found: {glue_script_path}")
        return {}
    
    with open(glue_script_path, 'r') as f:
        content = f.read()
    
    # Extract CATALOG_TABLES dictionary
    tables_match = re.search(r"CATALOG_TABLES\s*=\s*{([^}]+)}", content, re.DOTALL)
    if not tables_match:
        print("❌ CATALOG_TABLES not found in Glue script")
        return {}
    
    tables_content = tables_match.group(1)
    
    # Parse table definitions
    table_pattern = r"'([^']+)':\s*'([^']+)'"
    tables = {}
    for match in re.finditer(table_pattern, tables_content):
        key = match.group(1)
        value = match.group(2)
        tables[key] = value
    
    return tables

def extract_table_names_from_catalog_script():
    """Extract table names from the catalog setup script"""
    catalog_script_path = Path("scripts/setup_all_catalog_tables.py")
    
    if not catalog_script_path.exists():
        print(f"❌ Catalog script not found: {catalog_script_path}")
        return []
    
    with open(catalog_script_path, 'r') as f:
        content = f.read()
    
    # Extract expected_tables list
    tables_match = re.search(r"expected_tables\s*=\s*\[([^\]]+)\]", content, re.DOTALL)
    if not tables_match:
        print("❌ expected_tables not found in catalog script")
        return []
    
    tables_content = tables_match.group(1)
    
    # Parse table names
    table_pattern = r"'([^']+)'"
    tables = []
    for match in re.finditer(table_pattern, tables_content):
        tables.append(match.group(1))
    
    return tables

def main():
    """Main verification function"""
    print("Verifying Table Names Consistency")
    print("=" * 50)
    
    # Extract table names from both scripts
    glue_tables = extract_table_names_from_glue_script()
    catalog_tables = extract_table_names_from_catalog_script()
    
    print(f"\nGlue Script Tables ({len(glue_tables)}):")
    for key, value in glue_tables.items():
        print(f"  • {key}: {value}")
    
    print(f"\nCatalog Script Tables ({len(catalog_tables)}):")
    for table in catalog_tables:
        print(f"  •{table}")
    
    # Check consistency
    print(f"\nConsistency Check:")
    
    # Check if all Glue tables exist in catalog
    missing_in_catalog = []
    for key, table_name in glue_tables.items():
        if table_name not in catalog_tables:
            missing_in_catalog.append(table_name)
    
    # Check if there are extra tables in catalog
    glue_table_names = list(glue_tables.values())
    extra_in_catalog = [t for t in catalog_tables if t not in glue_table_names]
    
    if not missing_in_catalog and not extra_in_catalog:
        print("✅ All table names are consistent!")
        print("✅ Glue script will find all required catalog tables")
        return True
    else:
        if missing_in_catalog:
            print(f"❌ Tables missing in catalog setup:")
            for table in missing_in_catalog:
                print(f"  • {table}")
        
        if extra_in_catalog:
            print(f"⚠️ Extra tables in catalog (not used by Glue):")
            for table in extra_in_catalog:
                print(f"  • {table}")
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 