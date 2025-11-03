from .utils.wordlist_processor import load_data
from .utils.wordlist_processor import process_data
from pathlib import Path

def main():
    package_dir = Path(__file__).parent

    wordlist_file_path = f"{package_dir}/data/wordlist.csv"
    results_file_path = f"{package_dir}/data/results.csv"
    not_found_file_path = f"{package_dir}/data/not_found.txt"

    process_data(load_data(wordlist_file_path), results_file_path, not_found_file_path)
    

if __name__ == "__main__":
    main()