import re
import sys
import csv
from collections import defaultdict
import random
import os

DEBUG = True  # Set this to False to turn off debug messages

def debug_print(*args, **kwargs):
    if DEBUG:
        print("DEBUG:", *args, **kwargs)

# Function to format the input text and filter details with percentage >= 10%
def format_city_data(input_data):
    # Define the pattern to match city headers
    city_header_pattern = r'([^\(\):]+?)\s*\((\d+) application applied\):'

    # Find all matches of city headers and their positions
    matches = list(re.finditer(city_header_pattern, input_data))

    city_blocks = []

    # Extract the text between city headers as city blocks
    for i in range(len(matches)):
        start = matches[i].start()
        if i + 1 < len(matches):
            end = matches[i+1].start()
        else:
            end = len(input_data)
        block = input_data[start:end].strip()
        city_blocks.append(block)

    processed_data = []

    # Process each city block
    for block in city_blocks:
        city_header_match = re.match(city_header_pattern, block)
        if city_header_match:
            account = city_header_match.group(1).strip()
            applications = city_header_match.group(2).strip()

            # Extract the details
            details_text = block[len(city_header_match.group(0)):]
            # Split the details by '- '
            details = details_text.split('- ')[1:]  # Skip the first empty split
            for detail in details:
                # Find percentages in the detail
                percentage_match = re.search(r'(\d+)%', detail)
                if percentage_match:
                    percentage = int(percentage_match.group(1))
                    # Only include details with percentage >= 10%
                    if percentage >= 10:
                        # Extract the issue and count
                        issue_match = re.match(r'(.*?):\s*(\d+)\s*\(\d+%\)', detail.strip())
                        if issue_match:
                            issue = issue_match.group(1).strip().replace(" ", "_")
                            count = int(issue_match.group(2))
                            processed_data.append({
                                'account': account,
                                'issue': issue,
                                'count': count,
                                'percentage': percentage
                            })
    return processed_data

def read_csv_file(file_path):
    data = defaultdict(lambda: defaultdict(list))
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            account_id = row['account_id']
            _id = row['_id']
            req = row['req']
            for issue in row:
                if issue not in ['_id', 'account_id', 'req'] and row[issue] == '1':
                    data[account_id][issue.replace(" ", "_")].append((_id, req))
    return data

def find_matching_ids(data, account, issue, count):
    if account not in data:
        debug_print(f"No data found for account: {account}")
        return []

    if issue not in data[account]:
        debug_print(f"No data found for issue '{issue}' in account '{account}'")
        return []

    matching_records = data[account][issue]
    return random.sample(matching_records, min(count, len(matching_records)))

def generate_query(issue, results):
    if issue == "0_attachments_on_application":
        ids = [f"'{r}'" for r in results.get('ids', [])]
        return f"""
var results = db.getCollection('application').find({{
    _id: {{ 
        $in: [{', '.join(ids)}]
    }},
    $or: [
        {{ "error": "no resume data retrieved" }},
        {{ "new_attachments": {{ $size: 0 }} }}
    ]
}});
results.forEach(function(doc) {{
    var conditionMet = "";
    if (doc.error === "no resume data retrieved") {{
        conditionMet = "Error Message";
    }}
    if (doc.new_attachments && doc.new_attachments.length === 0) {{
        conditionMet += (conditionMet ? " & " : "") + "New Attachments Size 0";
    }}
    print("ID: " + doc._id + " - Condition(s) Met: " + conditionMet);
}});
"""
    elif issue == "invalid_req":
        reqs = [f"'{r}'" for r in results.get('reqs', [])]
        return f"""
db.getCollection('req').find({{ external_id: {{ $in: [{', '.join(reqs)}] }} }}, {{is_intern:1}})
"""
    elif issue == "invalid_resume":
        ids = [f"'{r}'" for r in results.get('ids', [])]
        return f"""
var exists = db.getCollection('application').find({{
    _id: {{
        $in: [{', '.join(ids)}]
    }},
    "error": "invalid resume"
}}).count() == {len(ids)};
print(exists);
"""
    elif issue == "unknown_error":
        ids = [f"'{r}'" for r in results.get('ids', [])]
        return f"""
const results = db.getCollection('application').find(
    {{
        $and: [
            {{ _id: {{ $in: [{', '.join(ids)}] }} }},
            {{ $or: [
                {{ error: {{ $exists: true }} }},
                {{ error: "no resume is usable" }}
            ]}}
        ]
    }},
    {{ _id: 1, error: 1 }}  // Projection to include only _id and error fields
);
results.forEach(doc => {{
    if (doc.error) {{
        print(`ID: ${{doc._id}}, Error: ${{doc.error}}`);
    }} else {{
        print(`ID: ${{doc._id}}, Error: No specific error message`);
    }}
}});
"""
    else:
        # Default query for cases where there's no specific query defined
        ids = [f"'{r}'" for r in results.get('ids', [])]
        return f"""
db.getCollection('application').find({{_id: {{$in: [{', '.join(ids)}]}}}})
"""

def get_valid_file_path():
    while True:
        file_path = input("Please enter the path to your CSV file: ").strip()
        if os.path.isfile(file_path) and file_path.lower().endswith('.csv'):
            return file_path
        else:
            print("Invalid file path. Please ensure the file exists and is a CSV file.")

def main():
    try:
        # Get the CSV file path
        csv_file_path = get_valid_file_path()

        # Read the CSV data
        data = read_csv_file(csv_file_path)
        debug_print("Data loaded from CSV:", data)

        # Read the input text from stdin
        print("Please paste the text and then press Ctrl+D (or Ctrl+Z on Windows) to finish:")
        input_data = sys.stdin.read()

        # Process the input data to extract relevant details
        processed_input = format_city_data(input_data)
        debug_print("Processed input:", processed_input)

        if not processed_input:
            print("No valid input data was processed. Please check your input format.")
            return

        results = defaultdict(lambda: defaultdict(list))
        for item in processed_input:
            account = item['account']
            issue = item['issue']
            count = item['count']
            matching_records = find_matching_ids(data, account, issue, min(5, count))
            if issue == "invalid_req":
                unique_reqs = list(set(record[1] for record in matching_records))  # Ensure unique req IDs
                results[account][issue] = {
                    'reqs': unique_reqs
                }
            else:
                results[account][issue] = {
                    'ids': [record[0] for record in matching_records]  # Return only _id
                }

        print("\nResults\n ----------------------\n")
        for account, issues in results.items():
            for issue, data in issues.items():
                if issue == "invalid_req":
                    reqs_formatted = " , ".join(f"'{req}'" for req in data['reqs'])
                    print(f"{account} - {issue}:")
                    print(reqs_formatted)
                else:
                    ids_formatted = " , ".join(f"'{id}'" for id in data['ids'])
                    print(f"{account} - {issue}:")
                    print(ids_formatted)

                # Generate and print the corresponding query
                query = generate_query(issue, data)
                print("\nCorresponding Query:")
                print(query)
                print()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        if DEBUG:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
