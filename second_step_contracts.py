import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from shared_functions import extract_dates_re, header_info, read_filing, search_phrases, search_phrases_with_context
from shared_functions import extract_text_with_timeout_doc, include_document, split_documents, dates_before_filing_date, strip_decimal


# Set path and parameters:
## The path to the folder where the results of the previously executed screening_of_filings.py was saved and where the results of this script will be saved:
results_directory = r""
## Optional: provide a suffix for the saved result file (e.g. "8K" or "test"), should be the same as the one used in the screening_of_filings.py script:
result_form_type = 'test'
## The path to the metadata file (screening_results_ CSV file saved by "screening_of_filings_for_phrases.py"):
metadata_file_path = os.path.join(results_directory, f"screening_results_{result_form_type}.csv")


# Ask a prompt to the user whether or not delete previous batch files:
delete_previous_batches = input("Do you want to delete previously saved batch files (only type n if the previous execution was interrupted)? (y/n): ").strip().lower()
if delete_previous_batches == 'y':
    # Delete all batch files in the results directory
    for filename in os.listdir(results_directory):
        if filename.startswith("results_batch_") and filename.endswith(".csv"):
            os.remove(os.path.join(results_directory, filename))
            print(f"Deleted {filename}")


# Parameters
# Lists of attachment <TYPE> and <TEXT> to exclude (we have verified that these are not loan agreements):
excluded_types=["GRAPHIC", "EXCEL", "ZIP", "XML"]
excluded_texts=["<XBRL>","<PDF>"]

# List of phrases that are if found in <DESCRIPTION> the attachment is not parsed (indicative of non-loan contracts)
excluded_descriptions=['CONSENT', 'CHIEF', 'PRINCIPAL', 'CFO', 'INCENTIVE', 'ANNUAL', 
                       'OFFICER', 'CERTIFICATION', 'EMPLOYMENT', 'EXECUTIVE', 'INDEPENDENT', 'RELEASE', 
                       'CERTIFICATE', 'PURCHASE', 'PURSUANT', 'CHARGES', 'CEO', 'EARNINGS', 'RATIOS', 
                       'RATIO',  'SUBSIDIARIES']

# Timeout in seconds for each parser (does not seem to work with full text, needs to be tested)
timeout_seconds = 60
#parsers = ['lxml', 'beautifulsoup']
#parsers = ['lxml']
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

phrases_first_50lines= [ 'Credit Agreement', 'Credit Facility', 'Credit Facilities', 'Loan Agreement', 'Loan Facility', 
            'Loan Facilities', 'Bridge Agreement', 'Bridge Facility', 'Bridge Facilities',
            'Credit and Security Agreement', 'Credit and Guaranty Agreement', 'Loan and Security Agreement', 'Loan and Guaranty Agreement',
            'Credit & Security Agreement', 'Credit & Guaranty Agreement', 'Loan & Security Agreement', 'Loan & Guaranty Agreement']

# Add the lists of phrases and phrases_first_50lines to phrases_second_step
phrases_second_step = phrases + phrases_first_50lines


sfas_phrases_ddl=["SFAS 159", "Statement of Financial Accounting Standards 159", "ASC 825", "Accounting Standards Codification 825-10-25", "the fair value option", "The fair value option"]
sfas_phrases_new = [
    "FAS 159",    
    "FASB ASC Topic 825",
    "ASC Subtopic 825",    
    "SFAS No. 159",
    "Accounting Standards Codification 825"
    "Statement of Financial Accounting Standards No. 159",        
    "Accounting Standards Codification Subtopic 825-10",
    "Accounting Standards Codification Subtopic 825 10",    
    "Accounting Standards Codification Section 825"]



def dict_of_documents_with_phrases(metadata_file_path, phrases_second_step):
    # read the metadata file csv into a pd dataframe, all columns should be read as strings
    metadata_df = pd.read_csv(metadata_file_path, dtype=str)
    # only keep the following columns: "Search results", "filename", "SEQUENCE_tag"
    metadata_df = metadata_df[["Search results","Search results first 50 lines", "filename", "SEQUENCE_tag"]]  
    # Only keep atachments, that is, where the SEQUENCE_tag is not 1:
    metadata_df = metadata_df[metadata_df["SEQUENCE_tag"] != "1"]
    # Convert the values in the "Search results" column to strings
    metadata_df["Search results"] = metadata_df["Search results"].astype(str)
    metadata_df["Search results first 50 lines"] = metadata_df["Search results first 50 lines"].astype(str)
    # combine the "Search results" and "Search results first 50 lines" columns into a single column
    metadata_df["Search results"] = metadata_df["Search results"] + metadata_df["Search results first 50 lines"]
    metadata_df["SEQUENCE_tag"] = metadata_df["SEQUENCE_tag"].astype(str)
    metadata_df['SEQUENCE_tag'] = metadata_df['SEQUENCE_tag'].apply(strip_decimal)
    # Check the "Search results" column for the phrases in the phrases_second_step list. Only keep rows where at least one of the phrases is found
    metadata_df = metadata_df[metadata_df["Search results"].apply(lambda x: any(phrase in x for phrase in phrases_second_step))]    
    # Create a dictionary from filename and SEQUENCE_tag columns. The keys should be the filename and the values should be a list of SEQUENCE_tag values
    dict_of_filings_to_parse = metadata_df.groupby("filename")["SEQUENCE_tag"].apply(list).to_dict()
    print("Completed creating dictionary of documents with phrases")
    return dict_of_filings_to_parse


# Only keep paths in txt_file_paths where the path contains a key from the dict_of_filings_to_parse
dict_of_filings_to_parse = dict_of_documents_with_phrases(metadata_file_path, phrases_second_step)
# print the number of keys in the dict_of_filings_to_parse dictionary
print(len(dict_of_filings_to_parse))
# Save the list of filepaths from dict_of_filings_to_parse to a list called txt_file_paths
txt_file_paths=list(dict_of_filings_to_parse.keys())


# Function to find the distance (in number of words) between a phrase and the first "Table of Contents"-type phrase
def dates_and_sfas_phrases(extracted_text):
    dates_and_sfas_phrases_result = {}
    #split and join the text to remove extra white spaces that are not line breaks:
    extracted_text = ' '.join(extracted_text.split())
    # Search for SFAS phrases
    dates_and_sfas_phrases_result["SFAS_phrase_ddl"]= search_phrases_with_context(extracted_text, sfas_phrases_ddl)
    dates_and_sfas_phrases_result["SFAS_phrase_new"]= search_phrases_with_context(extracted_text, sfas_phrases_new)

    # Search for dates and the amendment phrases
    text_to_search_list = extracted_text.split()
    text_to_search = ' '.join(text_to_search_list)
    dates_and_sfas_phrases_result.update(extract_dates_re(text_to_search))
    dates_and_sfas_phrases_result['amended'] = search_phrases(text_to_search.lower(), ["amended", "restated"])
    
    return dates_and_sfas_phrases_result



# Function to process a document
def process_document(document, parser_name, SEQUENCE_tag_values):
    result = {}
    try:
        # Skip a document if certain tags are found in the header
        if include_document(document, result, excluded_types, excluded_texts, excluded_descriptions, SEQUENCE_tag_values, document_header_tags):
            try:
                extracted_text, ishtml = extract_text_with_timeout_doc(parser_name, document)
            except Exception as e:
                result["error"] = f"Error extracting text: {str(e)}"
                return result

            try:
                split_text = extracted_text.split()
                result["html"] = ishtml
                result["Word count"] = len(split_text)
            except Exception as e:
                result["error"] = f"Error processing extracted text: {str(e)}"
                return result

            try:
                search_results = search_phrases(' '.join(split_text), phrases_second_step)
                result["Search results"] = search_results
            except Exception as e:
                result["error"] = f"Error searching phrases: {str(e)}"
                return result

            try:
                dates_and_sfas_dict = dates_and_sfas_phrases(extracted_text)
                result.update(dates_and_sfas_dict)
            except Exception as e:
                result["error"] = f"Error updating result with dates and SFAS phrases: {str(e)}"
                return result
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
    
    return result

# Function to process a file
def process_file(file_path, max_document_threads, dict_of_filings_to_parse):
    print(file_path)
    parser_name = 'lxml'
    results = []
    # find the SEQUENCE_tag values in the dict_of_filings_to_parse dictionary associated with the filename    
    try:                
        SEQUENCE_tag_values = dict_of_filings_to_parse[file_path]        

    except KeyError as e:
        return [{"error": f"KeyError: {str(e)}", "filename": file_path}]
    except Exception as e:
        return [{"error": f"Unexpected error: {str(e)}", "filename": file_path}]    
    try:
        filing_text = read_filing(file_path)
    except Exception as e:
        return [{"error": f"Error reading filing: {str(e)}", "filename": file_path}]
    
    try:
        documents, split_tag = split_documents(filing_text)
    except Exception as e:
        return [{"error": f"Error splitting documents: {str(e)}", "filename": file_path}]
    try:
        with ThreadPoolExecutor(max_workers=max_document_threads) as executor:
            futures = []
            for document in documents[1:]:
                futures.append(executor.submit(process_document, document, parser_name, SEQUENCE_tag_values))
            try:
                pre_document_result = header_info(documents[0], header_phrases)
            except Exception as e:
                return [{"error": f"Error processing header info: {str(e)}", "filename": file_path}]           
                            
            for future in as_completed(futures):
                try:
                    document_result = future.result()
                except Exception as e:
                    results.append({"error": f"Error getting future.result: {str(e)}", "filename": file_path})
                    continue
                try:
                    document_result.update(pre_document_result)
                    #print(document_result)
                    #raise Exception("Interrupting execution")
                except Exception as e:
                    results.append({"error": f"Error updating document result: {str(e)}", "filename": file_path})
                    continue
                try:
                    document_result["filename"] = file_path
                except Exception as e:
                    results.append({"error": f"Error setting filename: {str(e)}", "filename": file_path})
                    continue

                try:
                    document_result["split_tag"] = split_tag
                except Exception as e:
                    results.append({"error": f"Error setting split_tag: {str(e)}", "filename": file_path})
                    continue
                try:
                    # Save the dates that are in MDY_date and occur before or on the "FILED AS OF DATE:" 
                    dates_before_filing=dates_before_filing_date(document_result)
                    document_result.update(dates_before_filing)
                except Exception as e:
                    results.append({"error": f"Error appending result date candidates: {str(e)}", "filename": file_path})  
                # append the results to the results list if the document_result["SEQUENCE_tag"] is in the SEQUENCE_tag_values list
                try:
                    if document_result["SEQUENCE_tag"] in SEQUENCE_tag_values:
                        results.append(document_result)
                except Exception as e:
                    results.append({"error": f"Error appending result: {str(e)}", "filename": file_path})  
    except Exception as e:
        results = [{"error": str(e), "filename": file_path}]    
    return results


def main(dict_of_filings_to_parse, all_files, results_directory, result_form_type, batch_size, max_file_threads, max_document_threads):
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
            futures = [executor.submit(process_file, filename, max_document_threads, dict_of_filings_to_parse) for filename in batch_files]
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

    combined_csv = os.path.join(results_directory, f"second_step_contracts_{result_form_type}.csv")
    combined_df.to_csv(combined_csv, index=False)
    total_time = time.time() - start_time_total
    print(f"Total time taken: {total_time} seconds")

time.sleep(2)

main(dict_of_filings_to_parse, txt_file_paths, results_directory, result_form_type, batch_size=2000, max_file_threads=3, max_document_threads=1)