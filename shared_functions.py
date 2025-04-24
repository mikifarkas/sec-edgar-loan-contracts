from lxml import html
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import re
import os

# Timeout in seconds for each parser (does not seem to work with full text, needs to be tested)
timeout_seconds = 60

def strip_decimal(x):
    if x.endswith('.0'):
        return x[:-2]
    return x


def dates_before_filing_date(result):
    dates_before_filing={}
    if "FILED AS OF DATE:" in result and "MDY_dates" in result:
        filed_as_of_date = result["FILED AS OF DATE:"]
        # check filed_as_of_date is an 8 digit number:
        if len(filed_as_of_date) == 8 and filed_as_of_date.isdigit():        
            MDY_dates = result["MDY_dates"]
            # split it up based on "-", find day, month and year of MDY_dates:      
            MDY_dates_before_filed_as_of_date = [date for date in MDY_dates if int(date.split('-')[0])*100+int(date.split('-')[1])+int(date.split('-')[2])*10000 <= int(filed_as_of_date)]
            dates_before_filing["MDY_dates_before_filed_as_of_date"] = MDY_dates_before_filed_as_of_date
    return dates_before_filing

# Function that finds the <DOCUMENT> tags and splits up the text into individual documents/attachments/exhibits
def split_documents(filing_text):
    doc_start_pattern = re.compile(r'<DOCUMENT>', re.IGNORECASE)
    doc_end_pattern = re.compile(r'</DOCUMENT>', re.IGNORECASE)
    doc_start_positions = [m.start() for m in doc_start_pattern.finditer(filing_text)]
    doc_end_positions = [m.end() for m in doc_end_pattern.finditer(filing_text)]
    
    # Initialize the flag variable
    tags_alternate_correctly = True

    if len(doc_start_positions) != len(doc_end_positions):
        tags_alternate_correctly = False

    # Check that tags alternate correctly
    for i in range(len(doc_start_positions) - 1):
        if doc_start_positions[i] > doc_end_positions[i]:
            tags_alternate_correctly = False
            break
    
    documents = []
    
    # Add the header before the first <DOCUMENT> tag as the first document
    if doc_start_positions:
        header = filing_text[:doc_start_positions[0]]
        documents.append(header)
    
    for start_pos, end_pos in zip(doc_start_positions, doc_end_positions):
        documents.append(filing_text[start_pos:end_pos])
    
    return documents, tags_alternate_correctly


def include_document(doc, result, excluded_types, excluded_texts, excluded_descriptions, SEQUENCE_tag_values, document_header_tags):    
    result.update(attachment_header_info(doc, document_header_tags))
    # if the <SEQUENCE> tag is not in the SEQUENCE_tag_values list, skip the document
    if result["SEQUENCE_tag"] not in SEQUENCE_tag_values:
        return False
    try:
        if result["<TYPE>"] in excluded_types or result["<TEXT>"] in excluded_texts:
            return False  # Skip this document
        description_words = result.get("<DESCRIPTION>", "").split()       
        i = 0
        while i < len(description_words):
            if description_words[i] in excluded_descriptions:                
                return False  # Skip this document
            i += 1
    except:
        pass
    return True  # Continue processing this document



# Function to extract the header information from a document
def attachment_header_info(doc, document_header_tags):
    header_info_dict = {}
    for tag in document_header_tags:
        
        tag_positions = phrase_position(doc, tag)
        if tag_positions:
            tag_end_position = tag_positions[0] + len(tag)
            end_of_line_position = doc.find("\n", tag_end_position + 1)
            if end_of_line_position != -1:
                # Check if the end_of_line_position is within the next 20 words
                words_after_tag = doc[tag_end_position:end_of_line_position].split()
                if len(words_after_tag) > 20:
                    end_of_line_position = doc.find(' '.join(words_after_tag[:20]), tag_end_position) + len(' '.join(words_after_tag[:20]))
            if end_of_line_position != -1:
                word = doc[tag_end_position:end_of_line_position].strip()
                #from tag remove the < and > characters and add "_tag" to the tag name
                tag = tag[1:-1] + "_tag"
                header_info_dict[tag] = word
    
    return header_info_dict


def normalize_whitespace(text):
    """
    Replaces various types of spaces (including non-breaking spaces and zero-width spaces)
    with a regular space.
    """
    whitespace_chars = [
        "\u00A0",  # Non-breaking space (&nbsp;)
        "\u2000",  # En quad
        "\u2001",  # Em quad
        "\u2002",  # En space
        "\u2003",  # Em space
        "\u2004",  # Three-per-em space
        "\u2005",  # Four-per-em space
        "\u2006",  # Six-per-em space
        "\u2007",  # Figure space
        "\u2008",  # Punctuation space
        "\u2009",  # Thin space
        "\u200A",  # Hair space
        "\u202F",  # Narrow no-break space
        "\u205F",  # Medium mathematical space
        "\u3000",  # Ideographic space (full-width space)
        "\u180E",  # Mongolian vowel separator (deprecated)
        "\u200B",  # Zero-width space
        "\u2060",  # Word joiner (zero-width no-break space)
    ]
    
    # Replace all whitespace variants with a standard space
    text = re.sub(f"[{''.join(whitespace_chars)}]", " ", text)
    
    # Normalize multiple spaces to a single space
    return re.sub(r'\s+', ' ', text).strip()


def extract_dates_re(text, year_min=1990, year_max=2025):
    text=normalize_whitespace(text)

    MDY_dates = set()  # Use a set to avoid duplicates
    MY_dates = set()  # Separate set for pattern 5 dates
    # Dictionary of months: convert month names to numbers. Only use first 3 letters of month name
    month_dict = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    
    month_dict_reverse = {
        1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun',
        7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'
    }

    # Define regular expressions
    regex_patterns = [        
        r"([a-zA-Z, _ ]{30})([0-9]{4})", # Pattern:   ___ day of March, 2005; January ____, 2015
        r"([a-zA-Z]+)[ ]*([0-9]{1,2})[ ]*,?[ ]*([0-9]{4})",   # Pattern: January 30, 2015        
        r"([0-9]{1,2})(.{0,30}?)([0-9]{4})", # Patterns: 30 January, 2015; 30th January 2015; 30th day of January, 2015
        r"\b(0?[1-9]|1[0-2])[-/.\s](0?[1-9]|[12][0-9]|3[01])[-/.\s](\d{4})\b" #Patterns: 01/30/2015; 01-30-2015; 01.30.2015; 01 30 2015
    ]
    
    all_matches = []
    
    # Apply regular expressions and collect all matches with their positions
    for i, pattern in enumerate(regex_patterns):        
        for match in re.finditer(pattern, text):
            month=None
            day=None
            year=None
            if i == 0:
                #extract the month and the year from the match
                text_before, year = match.groups()
                #make sure text_before contains at least one "_" character:
                #if text_before.count("_") == 0:
                #    continue
                month = next((month for month in month_dict.keys() if month in text_before.lower()), None)
            if i == 1:
                #extract the month and the year from the match
                month, day, year = match.groups()
            if i == 2:
                day, in_between, year = match.groups()                
                month = next((month for month in month_dict.keys() if month in in_between.lower()), None)
            if i == 3:
                month_num, day, year = match.groups()
                month = month_dict_reverse[int(month_num)]
            if month:
                if year_min <= int(year) <= year_max and month.lower()[:3] in month_dict:
                    all_matches.append((match.start(), (day, month.lower()[:3], year), i))

    # Sort matches by their position in the text
    all_matches.sort(key=lambda x: x[0])

    # Process only the first 10 matches and the last 10 matches:    
    for _, match, i in all_matches[:5]+all_matches[-5:]:        
        if i == 0:  
            date_str = f"{match[1]} {match[2]}"
            MY_dates.add(date_str)        
        elif i == 1 or i == 2 or i == 3:  
            date_str = f"{match[1]} {match[0]} {match[2]}"
            MDY_dates.add(date_str)  
    
    # Convert sets to lists
    MDY_dates_list_string = list(MDY_dates)
    MY_dates_list_string = list(MY_dates)

    # Convert MDY dates to standard MDY format
    unique_dates_set = set() 
    for date in MDY_dates_list_string:
        date_parts = date.split() 
        date_parts[0] = month_dict[date_parts[0][:3].lower()]       
        unique_dates_set.add('-'.join(date_parts))    
    unique_MDY_dates_list = list(unique_dates_set)
                
    # Convert MY dates to standard MY format
    unique_dates_set = set() 
    for date in MY_dates_list_string:
        date_parts = date.split()   
        date_parts[0] = month_dict[date_parts[0][:3].lower()]     
        unique_dates_set.add('-'.join(date_parts))    
    unique_MY_dates_list = list(unique_dates_set)

    return {
        'MDY_dates': unique_MDY_dates_list,
        'MY_dates': unique_MY_dates_list
    }
    

def phrase_position(filing_text, phrase):
    return [m.start() for m in re.finditer(re.escape(phrase), filing_text)]

# Function to extract the text remaining on a line in the header after a phrase
def header_info(filing_text, phrases):
    remaining_lines = {}
    # count number of lines in filing_text:
    num_lines = filing_text.count('\n')-1
    first_100_lines = '\n'.join(filing_text.split('\n')[:min(100, num_lines)])
    for phrase in phrases:
        phrase_positions = phrase_position(first_100_lines, phrase)
        if phrase_positions:
            phrase_end_position = phrase_positions[0] + len(phrase)
            if phrase_end_position < len(filing_text):
                end_of_line_position = filing_text.find("\n", phrase_end_position + 1)
            if end_of_line_position != -1:
                word = filing_text[phrase_end_position + 1:end_of_line_position].strip()
                remaining_lines[phrase] = word
    return remaining_lines


def read_filing(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        # replace any characters that could not be decoded with white space:
        return file.read()

# Function to search for phrases in the filing (case-sensitive)
def search_phrases(filing_text, phrases):
    return [phrase for phrase in phrases if phrase in filing_text]

    
# Function to search for phrases in the filing (case-sensitive)
# It should return a list in which each element contains the phrase and its surrounding text (+- 40 words)
def search_phrases_with_context(filing_text, phrases, context_words=40):
    results = []
    words = filing_text.split()
    
    for phrase in phrases:
        phrase_positions = phrase_position(filing_text, phrase)
        
        for pos in phrase_positions:
            # Convert character position to word index
            char_count = 0
            start_word_index = 0
            end_word_index = len(words)
            
            for i, word in enumerate(words):
                char_count += len(word) + 1  # +1 for the space
                if char_count > pos:
                    start_word_index = max(0, i - context_words)
                    end_word_index = min(len(words), i + context_words + 1)
                    break
            
            context = ' '.join(words[start_word_index:end_word_index])
            results.append((phrase, context))
    
    return results


# Function to extract text from an HTML file using different parsers
def extract_text_from_html_doc(parser_name, doc):
    ishtml=False
    if "<html>" in doc.lower() or "</div>" in doc.lower():
        if parser_name == 'lxml':
            tree = html.fromstring(doc)
            ishtml=True
            return tree.text_content(), ishtml
        elif parser_name == 'beautifulsoup':            
            soup = BeautifulSoup(doc, 'html5lib')
            return soup.get_text()
        else:
            raise ValueError(f"Unsupported parser: {parser_name}")
    else:
        return doc, ishtml

# Function to extract text from an HTML file using different parsers with a timeout
def extract_text_with_timeout_doc(parser_name, doc, timeout=timeout_seconds):
    with ThreadPoolExecutor() as executor:
        future = executor.submit(extract_text_from_html_doc, parser_name, doc)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            raise TimeoutError(f"Parser {parser_name} timed out after {timeout} seconds")
        

# Function to get the paths of all the text files in the base directory
def get_txt_file_paths(base_directory, years, filing_types, quarters):
    txt_file_paths = []
    for year in years:        
        for filing_type in filing_types:
            for quarter in quarters:                             
                filings_directory = os.path.join(base_directory, year, filing_type, quarter)
                for root, dirs, files in os.walk(filings_directory):
                    for file in files[:]:
                        if file.endswith(".txt"):                            
                            txt_file_paths.append(os.path.join(root, file))
    return txt_file_paths