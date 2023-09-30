'''The logic of this script:
Look up reversed fqdns.
If today is the 1st day of the month, create a file named "month_YYYY.csv"; otherwise, use the file for this month.
Create a dictionary mapping the reversed fqdn to the normal one (if found) and write them in 'updated_values.csv'.
Create a new file 'dns_result_{FROM_DATE}' replacing reversed fqdns with normal ones.
Count the number of occurrences and write it to a CSV file.
Map fqdns to server providers (reading from 'file_to_match' CSV file).
When finding the right value, check what row it is.
Take the values from this exact row and put them in the final CSV columns 3, 4, 5, 6.
Append today's data into a monthly file.
Check that the names of the queries are unique and sum up their values.
If today is the last day of the month, append this month's data to the final 'dns_full_data.csv' file and check if the names of the queries are unique. Otherwise, continue the above logic.'''

import csv
from datetime import date, timedelta
import datetime
import subprocess
import logging
import csv
import os
import collections

logging.basicConfig(filename='csv_transformation.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
today = date.today()
yesterday = today - timedelta(days=1)
FROM_DATE = yesterday.strftime("%Y-%m-%d")
TO_DATE = today.strftime("%Y-%m-%d")
original_csv_file = f'dns_result_original_{FROM_DATE}.csv'
file_to_match = 'file_to_match.csv'
final_file = f'dns_result_{FROM_DATE}.csv'



def is_first_day_of_month():
    """
    Check if today is the first day of the month and create a new CSV file named "month_YYYY.csv."

    This function checks the current date and determines if it's the first day of the month.
    If it is, a new CSV file is created with a name in the format "month_YY.csv," where "month" is the full
    name of the month and "YY" represents the current year in lowercase.

    Returns:
        str: The name of the created CSV file if it's the first day of the month.

    Note:
        - The created CSV file will have a header row with columns 'query', 'total quantity', 'service provider name', and 'sid'.
        - This function uses the current date and time to determine if it's the first day of the month.
    """
    first_of_month = datetime.datetime.now()
    if first_of_month.day == 1:
        month_year = first_of_month.strftime("%B_%Y")
        month_file_name = f"{month_year.lower()}.csv"
        with open(month_file_name, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['query', 'total quantity', 'service provider name', 'sid'])
        logging.info('is_first_day_of_month(): Created a csv file for the current month')
        return month_file_name


def reversed_values_lookup():
    """
    Search for matches for reversed queries and implement lookup commands.

    This function reads a CSV file and searches for queries containing "in-addr.arpa" in the first column.
    For each matching query, it runs a `host` command to perform a reverse DNS lookup and extract the result.
    The results are then written to a new CSV file named 'updated_values.csv' with a mapping of original queries
    and their corresponding reverse lookup results.

    Returns:
        None

    Note:
        - This function uses the 'subprocess' module to execute the `host` command.
        - Results of successful lookups are written to 'updated_values.csv' with two columns: original query and result.
        - Errors during command execution are logged as errors.
    """
    mapping = []
    with open(original_csv_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            words = line.strip().split(',')[0]
            if "in-addr.arpa" in words:
                command = f"host -t ptr {words}"
                result = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)
                if result.returncode == 0:
                    logging.info('reversed_values_lookup(): Command executed successfully:%s', result.stdout )
                    split_parts = result.stdout.split("pointer")
                    if len(split_parts) > 1:
                        value_after_pointer = split_parts[1].strip()
                        logging.info('reversed_values_lookup():%s', value_after_pointer)
                        mapping.append((words, value_after_pointer))
                else:
                    logging.error('reversed_values_lookup(): Command execution failed %s', result.stderr)
    with open('updated_values.csv', 'w', newline='', encoding='utf-8') as output_file:
        csvwriter = csv.writer(output_file)
        csvwriter.writerows(mapping)
    logging.info('reversed_values_lookup(): Iterated reversed FQDNs and looked up the normal values for it')
    return None


def replace_found_fqdns():
    """
    Replace reversed FQDNs with their corresponding normal FQDNs.

    This function reads a CSV file containing a mapping of reversed FQDNs to normal FQDNs.
    It then reads another CSV file with original data, and for each row, it checks if the FQDN is in the mapping.
    If a match is found, it replaces the original FQDN with the corresponding normal FQDN.
    The updated rows are written to a new CSV file.

    Returns:
        None

    Note:
        - The function reads mapping data from 'updated_values.csv' and original data from 'original_csv_file'.
        - Rows with no matching reversed FQDNs are skipped.
        - The updated data is written to 'final_file'.
    """
    mapping = {}
    with open('updated_values.csv', 'r', encoding='utf-8') as mapping_file:
        mapping_reader = csv.reader(mapping_file)
        for row in mapping_reader:
            key = row[0]
            value = row[1]
            mapping[key] = value
    with open(original_csv_file, 'r', encoding='utf-8') as origins_file:
        origins_reader = csv.reader(origins_file)
        rows = list(origins_reader)
    updated_rows = []
    for row in rows:
        if row[0] in mapping:
            new_value = mapping[row[0]]
            row[0] = new_value
        updated_rows.append(row)
    with open(final_file, 'w', newline='', encoding='utf-8') as updated_file:
        csvwriter = csv.writer(updated_file)
        csvwriter.writerows(updated_rows)
    logging.info('replace_found_fqdns(): Replaced found reversed FQDNs')
    return None


def count_queries_from_csv(final_file):
    """
    Count the number of occurrences of each query from an input CSV and write the counts to an output CSV file.

    This function reads queries and their counts from an input CSV file, and it counts the occurrences of each query.
    The counts are then written to an output CSV file.

    Args:
        final_file (str): The path to the input CSV file containing queries and their counts.

    Returns:
        None

    Note:
        - The function uses the 'collections.Counter' to efficiently count query occurrences.
        - It assumes that the input CSV file has two columns: query and count.
        - The counts are written to the same 'final_file' after aggregation.
    """
    value_counts = collections.Counter()
    # Read queries from the input CSV file
    with open(final_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 2:
                query = row[0]  # Assuming the query is in the first column of the CSV
                count = int(row[1])  # Assuming the count is in the second column
                value_counts[query] += count

    # Write the counts to the output CSV file
    with open(final_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for value, count in value_counts.items():
            if len(value) > 3:
                writer.writerow([value, count])
    logging.info('count_queries_from_csv(): Count tte number of FQDN occurencies')
    return None


def update_csv_file_with_columns(file_to_match):
    """
    Map values from a generated file to a file that includes service provider name, subdomain, environment, and SID.

    This function reads data from two CSV files. The first file ('final_file') contains data with queries,
    and the second file ('file_to_match') includes additional information such as service provider name, subdomain,
    environment, and SID. The function maps values from the second file to the first file based on a matching condition,
    and it updates the columns of the first file with the mapped values.

    Args:
        file_to_match (str): The path to the CSV file containing data to be mapped and added to 'final_file.'

    Returns:
        None

    Note:
        - The function iterates through both CSV files to perform the mapping based on a matching condition.
        - The columns in 'final_file' are updated with the mapped values from 'file_to_match.'
        - The updated data is written back to 'final_file.'
    """
    with open(file_to_match, 'r', encoding='utf-8') as csvfile2:
        csv_reader2 = csv.reader(csvfile2)
        file2_data = [row for row in csv_reader2]
    updated_data = []
    with open(final_file, 'r', encoding='utf-8') as csvfile1:
        csv_reader1 = csv.reader(csvfile1)
        for row in csv_reader1:
            updated_row = row[:]
            for provider_row in file2_data:
                print(provider_row)
                if provider_row[-1] in row[0]:
                    print(provider_row[0])
                    updated_row.append(provider_row[0])
                    updated_row.append(provider_row[1])
                    break
            updated_data.append(updated_row)
    with open(final_file, 'w', newline='', encoding='utf-8') as csvfile1:
        csv_writer = csv.writer(csvfile1)
        csv_writer.writerows(updated_data)
    logging.info('update_csv_file_with_columns(): Mapped the FQDNs to the values in match_file and updated columns')
    return None


def append_csv(final_file, month_file_name):
    """
    Append today's data from a source CSV file to a monthly CSV file.

    This function reads data from a source CSV file ('final_file') and appends it to a monthly CSV file ('month_file_name').
    The source file's data is appended to the end of the destination file.

    Args:
        final_file (str): The path to the source CSV file containing today's data.
        month_file_name (str): The path to the monthly CSV file to which data will be appended.

    Returns:
        None

    Note:
        - The function uses the 'csv' module to read and write CSV data.
        - If an error occurs during the operation, it is logged as an error.
    """
    try:
        with open(final_file, "r", newline="", encoding='utf-8') as source, open(month_file_name, "a", newline="", encoding='utf-8') as destination:
            source_reader = csv.reader(source)
            destination_writer = csv.writer(destination)
            # Append the data from the source to the destination file
            for row in source_reader:
                destination_writer.writerow(row)
        logging.info(f"append_csv(): Appended {final_file} to {month_file_name}")
        return None
    except Exception as e:
        logging.error(f"append_csv(): An error occurred: {str(e)}")


def count_appended(month_file_name):
    """
    When today's data is appended to a monthly file, revisit query names and sum up their quantity if found.

    This function reads data from a monthly CSV file ('month_file_name') and revisits query names.
    For queries with numeric values, it sums up their quantities. Duplicate names are combined,
    and non-matching rows are preserved in the updated file.

    Args:
        month_file_name (str): The path to the monthly CSV file containing appended data.

    Returns:
        None

    Note:
        - The function reads and updates data in the same CSV file.
        - Numeric values are detected by checking if they can be converted to a numeric type.
        - Duplicate names are combined, and non-matching rows are preserved.
    """
    name_data = {}
    with open(month_file_name, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader, None)
        for row in csv_reader:
            if len(row) >= 3:
                name = row[0]
                value_str = row[1]
                if value_str.replace('.', '', 1).isdigit():
                    value = (value_str)
                else:
                    value = None
                if name in name_data:
                    if value is not None:
                        name_data[name][1] += value
                else:
                    name_data[name] = row
    with open(month_file_name, 'w', newline='', encoding='utf-8') as csv_output:
        csv_writer = csv.writer(csv_output)
        if header:
            csv_writer.writerow(header)
        for name, data in name_data.items():
            csv_writer.writerow(data)
    logging.info(f"count_appended(): Data with duplicate names combined (if numeric) and non-matching rows preserved saved to {month_file_name}")
    return None


def if_last_day_of_the_month(month_file_name):
    '''check if today is the last day of the month and append the monthly data to the final DNS file dns_full_data.csv'''
    today = datetime.date.today()
    is_last_day_of_month = (today + datetime.timedelta(days=1)).day == 1
    if is_last_day_of_month:
        input_file = month_file_name
        output_file = f'dns_full_data.csv'
        with open(input_file, 'r', encoding='utf-8') as csv_input, open(output_file, 'a', newline='', encoding='utf-8') as csv_output:
            csv_reader = csv.reader(csv_input)
            csv_writer = csv.writer(csv_output)
            for row in csv_reader:
                csv_writer.writerow(row)
        logging.info('if_last_day_of_the_month(): Content from %s', {input_file}, 'has been written to %s', {output_file}, 'without a header row.')
        count_appended(output_file)
    return None


def clean_up():
    """
    Check if today is the last day of the month and append monthly data to the final DNS file 'dns_full_data.csv'.

    This function checks if today's date is the last day of the month. If it is, it appends the data from the
    specified monthly file ('month_file_name') to the final DNS data file ('dns_full_data.csv'). The data is appended
    without a header row.

    Args:
        month_file_name (str): The path to the monthly CSV file containing data to be appended.

    Returns:
        None

    Note:
        - The function checks if today's date is the last day of the month by adding one day and checking if the day is 1.
        - If it's the last day of the month, data is appended to 'dns_full_data.csv' without a header row.
        - The 'count_appended' function is called to process the appended data.
    """
    if os.path.exists(original_csv_file) and os.path.exists('updated_values.csv') and os.path.exists(final_file):
        os.remove(original_csv_file)
        os.remove(final_file)
        os.remove('updated_values.csv')
    logging.info("clean_up(): Removed an original file and temporarly ones")
    return None


if __name__ == "__main__":
    month_file_name='september_2023.csv'
    #is_first_day_of_month()
    reversed_values_lookup()
    replace_found_fqdns()
    count_queries_from_csv(final_file)
    update_csv_file_with_columns(file_to_match)
    append_csv(final_file, month_file_name)
    count_appended(month_file_name)
    #if_last_day_of_the_month(month_file_name)
    #clean_up()
    logging.info('Completed the script')
