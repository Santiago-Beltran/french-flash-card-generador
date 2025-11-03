import os
import requests
import concurrent.futures #multithreading
import pandas as pd


def load_data(csv_data_path):
    """
    This method is specifically designed to work with the data provided at 
    "https://docs.google.com/spreadsheets/d/1DblQvT6CI2uZSdR5s4uCqYrRcYsCH4pOH_OQT4KuI8I/edit?gid=0#gid=0"

    Download it as a csv file wherever you please.
    """

    df_words = pd.read_csv(csv_data_path, header=1, on_bad_lines='skip')
    df_words = df_words.iloc[:, :5]
    
    # Renaming columns
    df_words.columns = ["Frequency", "Lemme", "Audio Link", "Definition", "Context"]

    return df_words

def process_data(df_words, csv_results_file_path, not_found_file_path):

    """
    Encompasses all the data processing done.

    Receives a dataframe given by load data.
    """
    current_index = 0
    processed_rows_in_memory = []

     # Check if file exists, if not, generate header.
    if not os.path.exists(csv_results_file_path):
        header = ['Frequency', 'Lemma', 'Grammatical Function', 'Homograph Number', 'Definition URL', 'Sound URL', 'Homograph']
        df_header = pd.DataFrame(columns=header)
        df_header.to_csv(csv_results_file_path, mode='w', header=True, index=False)
        
    # Multithreading.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []  # Store future results
        
        for _, row in df_words.iloc[:100].iterrows():  
            futures.append(executor.submit(process_row, row, csv_results_file_path, not_found_file_path, processed_rows_in_memory, current_index))
        
        # Wait for all threads to complete and collect the results.
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                current_index = result  # Update the index from the result
    
    if processed_rows_in_memory:
        write_results(processed_rows_in_memory, csv_results_file_path)
    return

def find_dictionary_links(word):
    """
    Generates links to dictionary entries for the given word
    """

    endpoint = "https://www.dictionnaire-academie.fr/search"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.dictionnaire-academie.fr",
        "Referer": "https://www.dictionnaire-academie.fr/",
        "User-Agent": "Mozilla/5.0"
    }

    payload = {"term": word}
    r = requests.post(endpoint, data=payload, headers=headers)
    response_json = r.json()
    df_response = pd.DataFrame(response_json['result'])

    # A dictionary is given as an answer, its fields are:
    # url: Assigned to a each meaning of a word.
    # label: Searched word.
    # score: Similarity, from 0 to 1, of a given word to a word entry in the dictionary.
    # nbhomograph: Ranks, from more popular to least, meaning of words.

    # Only perfect matches allowed.
    df_response = df_response[df_response['score'] == '1.0']

    return df_response

def write_results(rows, csv_filepath):
    """
    Writes processed data to disk.

    Receives rows, list of processed rows.
    
    """

    df = pd.concat(rows, ignore_index=True)

    # Reformatting

    df = df.rename(columns={'url': 'Definition Url', 'nbhomograph': 'Nbhomograph', 'nature': 'Grammatical Function', 'Recording Url' : 'Sound Link', 'multiple_results' : 'Multiple Results'})
    df = df.iloc[:, [4, 1, 3, 2, 0, 5, 6]]
    
    df.to_csv(csv_filepath, mode='a', header=False, index=False, encoding='utf-8')

    return

def write_not_found(word, path):
    with open(path, 'a') as file:  
        file.write(word + "\n")
    return

def process_row(row,  csv_results_file_path, not_found_file_path, processed_rows_in_memory=[], current_index=0, write_step=10):
    """
    Process a single row generating dictionary links for each word.

    If a word is not found in the dictionary, add it to the not_found.txt
    file separated by spaces.    

    Args:
        - row: Dataframe row with Frequency, Audio Link, Definition and Context columns.
        - expanded_rows: List containing previously processed rows stored in memory.
        - current_index: Last index processed.
        - write_step: How many rows to process before writing to disk.
    """

    frequency = row["Frequency"]
    word = row["Lemme"]
    dictionary_link_results = find_dictionary_links(word)

    if dictionary_link_results.empty:
        write_not_found(word, not_found_file_path)
        return
    
    # Temporary dataframe
    temp = dictionary_link_results.copy()
    temp["Frequency"] = frequency
    temp["Audio Link"] = f"https://forvo.com/word/{word}"

    # Classification by having homographes
    temp["Has Homographes"] = len(temp) > 1

    # Store in memory
    processed_rows_in_memory.append(temp)

    if current_index % write_step == 0:
        write_results(processed_rows_in_memory, csv_results_file_path)
        processed_rows_in_memory.clear()

    # Later used for parallelism.
    return current_index + 1