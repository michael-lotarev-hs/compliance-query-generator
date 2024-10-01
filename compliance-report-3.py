import argparse

def main():
    try:
        # Use argparse to accept file_path and text input from command line arguments
        parser = argparse.ArgumentParser(description="Process city data and generate MongoDB queries")
        parser.add_argument('--file_path', type=str, required=True, help="The path to the CSV file")
        parser.add_argument('--text', type=str, required=True, help="The text to process")

        args = parser.parse_args()
        csv_file_path = args.file_path
        input_data = args.text

        # Read the CSV data
        data = read_csv_file(csv_file_path)
        debug_print("Data loaded from CSV:", data)

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
