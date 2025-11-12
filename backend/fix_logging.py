#!/usr/bin/env python3
"""
Script to fix BigQuery query logging by replacing direct .client.query() calls
with the logging wrapper method.
"""
import re
import sys

def fix_data_service():
    """Fix all bq_service.client.query() calls in data_service.py"""
    file_path = 'services/data_service.py'

    with open(file_path, 'r') as f:
        content = f.read()

    # Pattern: bq_service.client.query(query).to_dataframe()
    # Replace with: bq_service._execute_and_log_query(query, query_type='data_service', endpoint='unknown')

    pattern = r'bq_service\.client\.query\(([^)]+)\)\.to_dataframe\(\)'
    replacement = r'bq_service._execute_and_log_query(\1, query_type="pivot", endpoint="data_service")'

    new_content = re.sub(pattern, replacement, content)

    with open(file_path, 'w') as f:
        f.write(new_content)

    count = len(re.findall(pattern, content))
    print(f"Fixed {count} occurrences in data_service.py")
    return count

def fix_bigquery_service():
    """Fix all self.client.query() calls in bigquery_service.py (outside _execute_and_log_query)"""
    file_path = 'services/bigquery_service.py'

    with open(file_path, 'r') as f:
        content = f.read()

    # We need to be careful not to replace the call inside _execute_and_log_query itself
    # Pattern: self.client.query(query).to_dataframe() but NOT inside _execute_and_log_query

    lines = content.split('\n')
    new_lines = []
    inside_execute_and_log = False

    for i, line in enumerate(lines):
        # Track if we're inside the _execute_and_log_query method
        if 'def _execute_and_log_query(' in line:
            inside_execute_and_log = True
        elif inside_execute_and_log and line and not line[0].isspace():
            # We've left the method (reached a non-indented line or new method)
            inside_execute_and_log = False

        # Replace self.client.query() calls OUTSIDE _execute_and_log_query
        if 'self.client.query(' in line and '.to_dataframe()' in line and not inside_execute_and_log:
            # Extract the query variable name
            match = re.search(r'self\.client\.query\(([^)]+)\)\.to_dataframe\(\)', line)
            if match:
                query_var = match.group(1)
                # Replace with logging call
                new_line = re.sub(
                    r'self\.client\.query\(([^)]+)\)\.to_dataframe\(\)',
                    r'self._execute_and_log_query(\1, query_type="info", endpoint="bigquery_info")',
                    line
                )
                new_lines.append(new_line)
                print(f"Line {i+1}: Replaced in bigquery_service.py")
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)

    new_content = '\n'.join(new_lines)

    with open(file_path, 'w') as f:
        f.write(new_content)

    print("Fixed bigquery_service.py")

if __name__ == '__main__':
    print("Fixing query logging...")
    count_data = fix_data_service()
    fix_bigquery_service()
    print(f"\nTotal fixes: {count_data} in data_service.py + 2 in bigquery_service.py")
    print("Done!")
