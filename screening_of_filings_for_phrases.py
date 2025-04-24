import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from shared_functions import header_info, read_filing, search_phrases, extract_text_with_timeout_doc, split_documents, attachment_header_info

# Issue with python 3.9 when writing to csv there was a "cannot escape character" error when writing a pd dataframe to csv --> on python 3.13 and the error dissapeared.


# Set paths and parameters:
## The path to the folder containing the filings (all txt files in the folder will be processed assuming they are filings):
folder_containing_filings = r""
## The path to the folder where the results will be saved:
results_directory = r""
## Filings type used to name the output file:
result_form_type = '8K'


#Build a list of file paths in txt_file_paths containing the filings: all .txt files in the folder_containing_filings and its subfolders:
txt_file_paths = []
for root, dirs, files in os.walk(folder_containing_filings):
    for file in files:
        if file.endswith('.txt'):
            txt_file_paths.append(os.path.join(root, file))


# Use a specific file for testing
# txt_file_paths = [r"...0000055785-15-000092.txt"]

# Ask a prompt to the user whether or not delete previous batch files:
delete_previous_batches = input("Do you want to delete previously saved batch files (only type n if the previous execution was interrupted)? (y/n): ").strip().lower()
if delete_previous_batches == 'y':
    # Delete all batch files in the results directory
    for filename in os.listdir(results_directory):
        if filename.startswith("results_batch_") and filename.endswith(".csv"):
            os.remove(os.path.join(results_directory, filename))
            print(f"Deleted {filename}")



# Parameters
# Lists of attachment types and texts to exclude
excluded_types=["GRAPHIC", "EXCEL", "ZIP", "XML"]
excluded_texts=["<XBRL>","<PDF>"]

# List of phrases that are if found in <DESCRIPTION> the attachment is not parsed (indicative of non-loan contracts)
excluded_descriptions=['CONSENT', 'CHIEF', 'PRINCIPAL', 'CFO', 'INCENTIVE', 'ANNUAL', 
                       'OFFICER', 'CERTIFICATION', 'EMPLOYMENT', 'EXECUTIVE', 'INDEPENDENT', 'RELEASE', 
                       'CERTIFICATE', 'PURCHASE', 'PURSUANT', 'CHARGES', 'CEO', 'EARNINGS', 'RATIOS', 
                       'RATIO',  'SUBSIDIARIES']

# Timeout in seconds for each parser (does not seem to work with full text, needs to be tested)
timeout_seconds = 60

#parser_name = 'beautifulsoup'
parser_name = 'lxml'

# Header phrases to search for in the header of the filing text and find their values
header_phrases = ["CENTRAL INDEX KEY:","COMPANY CONFORMED NAME:","CONFORMED SUBMISSION TYPE:", "CONFORMED PERIOD OF REPORT:", 
                  "FILED AS OF DATE:", "DATE AS OF CHANGE:", "PUBLIC DOCUMENT COUNT:", "STANDARD INDUSTRIAL CLASSIFICATION:"]

# Header tags to search for in attachments and find their values
document_header_tags = ["<TYPE>", "<DESCRIPTION>", "<TEXT>","<FILENAME>","<SEQUENCE>"]

# Loan phrases to search for
phrases = ["CREDIT AGREEMENT", "LOAN AGREEMENT", "CREDIT FACILITY", "LOAN AND SECURITY AGREEMENT", "LOAN & SECURITY AGREEMENT",
           "REVOLVING CREDIT", "FINANCING AND SECURITY AGREEMENT", "FINANCING & SECURITY AGREEMENT", "CREDIT AND GUARANTEE AGREEMENT",
           "CREDIT & GUARANTEE AGREEMENT", "NOTE AGREEMENT", "FACILITY AGREEMENT", "FACILITIES AGREEMENT", "FINANCING AGREEMENT",
           "CREDIT AND SECURITY AGREEMENT", "CREDIT & SECURITY AGREEMENT", "INTERCREDITOR AGREEMENT", 'CREDIT FACILITY', 'CREDIT FACILITIES',
             'LENDING FACILITY', 'LENDING FACILITIES', 'LOAN FACILITY', 'LOAN FACILITIES', 'BRIDGE FACILITY', 'BRIDGE FACILITIES',
             "BRIDGE NOTE", "LOAN NOTE", "MASTER NOTE"]

# Loan phrases to search for in the first 50 lines of the document
phrases_first_50lines= [ 'Credit Agreement', 'Credit Facility', 'Credit Facilities', 'Loan Agreement', 'Loan Facility', 
            'Loan Facilities', 'Bridge Agreement', 'Bridge Facility', 'Bridge Facilities',
            'Credit and Security Agreement', 'Credit and Guaranty Agreement', 'Loan and Security Agreement', 'Loan and Guaranty Agreement',
            'Credit & Security Agreement', 'Credit & Guaranty Agreement', 'Loan & Security Agreement', 'Loan & Guaranty Agreement']


def include_document(doc, result, excluded_types, excluded_texts, excluded_descriptions, document_header_tags):    
    result.update(attachment_header_info(doc, document_header_tags))
    try:
        if result["<TYPE>"] in excluded_types or result["<TEXT>"] in excluded_texts:
            return False  # Skip this document
        description_words = result.get("<DESCRIPTION>", "").split()
        #print(description_words)
        i = 0
        while i < len(description_words):
            if description_words[i] in excluded_descriptions:                
                return False  # Skip this document
            i += 1
    except:
        pass
    return True  # Continue processing this document


# Function to extract the text from the first 50 non-empty lines of a document:
def extract_first_50lines(doc):
    lines = doc.split('\n')
    non_empty_lines = [line for line in lines if line.strip()]
    first_50lines = non_empty_lines[:50]
    first_50lines = ' '.join(first_50lines)
    # remove additional while spaces
    first_50lines = first_50lines.split()
    return ' '.join(first_50lines)


# Function to process a document within a filing
def process_document(document, parser_name):
    # Initialize a dictionary to store the results
    result={}
    # Skip a document if certain tags are found in the header
    if  include_document(document, result, excluded_types, excluded_texts, excluded_descriptions, document_header_tags):
        try:
            extracted_text, ishtml = extract_text_with_timeout_doc(parser_name, document)
        except Exception as e:            
            return result        
            
        split_text = extracted_text.split()
        result["html"] = ishtml
        result["Word count"] = len(split_text)
        # Find capitalized loan phrases in the text        
        search_results = search_phrases(' '.join(split_text), phrases)
        result["Search results"] = search_results
        # Find the phrases in the first 50 lines of the document
        first_50lines = extract_first_50lines(extracted_text)
        search_results_first_50lines = search_phrases(first_50lines, phrases_first_50lines)        
        result["Search results first 50 lines"] = search_results_first_50lines

    return result


# Function to process a file
def process_file(file_path, max_document_threads, parser_name):
    print(file_path)    
    results = []
    filing_text = read_filing(file_path)
    documents, split_tag = split_documents(filing_text)    
    try:
        with ThreadPoolExecutor(max_workers=max_document_threads) as executor:
            futures = []
            for document in documents[1:]:
                futures.append(executor.submit(process_document, document, parser_name))
                        
            pre_document_result = header_info(documents[0], header_phrases)
                
            for future in as_completed(futures):
                document_result = future.result()
                document_result.update(pre_document_result)
                document_result["filename"] = file_path
                document_result["split_tag"] = split_tag
                results.append(document_result)
    except Exception as e:
        results = [{"error": str(e), "filename": file_path}]    
    return results

def main(all_files, results_directory, result_form_type, parser_name, batch_size, max_file_threads, max_document_threads):
    start_time_total = time.time()
    
    existing_batches = set()

    # Determine already processed batches
    for filename in os.listdir(results_directory):
        if filename.startswith("results_batch_") and filename.endswith(".csv"):
            batch_number = int(filename.split("_")[2].split(".")[0])
            existing_batches.add(batch_number)

    # Process files in batches
    for i in range(0, len(all_files), batch_size):
        batch_number = i // batch_size + 1
        if batch_number in existing_batches:
            print(f"Batch {batch_number} already processed. Skipping.")
            continue

        batch_files = all_files[i:i + batch_size]
        batch_results = []
#sleep for 3 seconds
        time.sleep(3)

        with ThreadPoolExecutor(max_workers=max_file_threads) as executor:
            futures = [executor.submit(process_file, filename, max_document_threads, parser_name) for filename in batch_files]
            for future in as_completed(futures):
                results = future.result()
                batch_results.extend(results)

        # Convert the list of dictionaries to a pandas DataFrame
        batch_df = pd.DataFrame(batch_results)
        #batch_df.replace({r'\n': ' ', r'\r': ' ', '"': ' '}, regex=True, inplace=True)
        batch_filename = os.path.join(results_directory, f"results_batch_{batch_number}.csv")
        batch_df.to_csv(batch_filename, index=False)

    # Combine all batch CSV files into a single DataFrame
    combined_df = pd.DataFrame()
    for i in range(0, len(all_files), batch_size):
        batch_filename = os.path.join(results_directory, f"results_batch_{i // batch_size + 1}.csv")
        batch_df = pd.read_csv(batch_filename)
        combined_df = pd.concat([combined_df, batch_df], ignore_index=True)

    # Write the combined results to a final CSV file

    combined_csv = os.path.join(results_directory, f"screening_results_{result_form_type}.csv")
    combined_df.to_csv(combined_csv, index=False)
    total_time = time.time() - start_time_total
    print(f"Total time taken: {total_time} seconds")

main(txt_file_paths, results_directory, result_form_type, parser_name, batch_size=2000, max_file_threads=3, max_document_threads=1)