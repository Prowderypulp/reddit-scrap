import json
import csv
import argparse
from pathlib import Path

def jsonl_to_csv(input_path: str, output_path: str):
    in_file = Path(input_path)
    out_file = Path(output_path)

    if not in_file.exists():
        print(f"Error: Could not find '{in_file}'. Make sure the scraper has finished running.")
        return

    # Define the columns we want to keep in the CSV
    fieldnames = [
        "id", 
        "author", 
        "subreddit", 
        "created_utc", 
        "score",       # Represents the upvotes
        "_keyword",    # The keyword from your txt file that found this comment
        "body"         # The actual text of the comment
    ]

    print(f"Parsing {in_file} into {out_file}...")

    with in_file.open('r', encoding='utf-8') as f_in, \
         out_file.open('w', encoding='utf-8', newline='') as f_out:
        
        # extrasaction='ignore' safely ignores any extra JSON fields not in our fieldnames list
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        count = 0
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                
                # Construct the row, providing defaults if a field is missing
                row = {
                    "id": data.get("id", ""),
                    "author": data.get("author", "[deleted]"),
                    "subreddit": data.get("subreddit", ""),
                    "created_utc": data.get("created_utc", ""),
                    "score": data.get("score", 0),  
                    "_keyword": data.get("_keyword", ""),
                    "body": data.get("body", "")
                }
                
                writer.writerow(row)
                count += 1
                
            except json.JSONDecodeError:
                print("Warning: Skipped an invalid JSON line.")
    
    print(f"Success! Converted {count} comments to CSV.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Arctic Shift JSONL comments to CSV")
    parser.add_argument("--input", default="arctic_out/comments.jsonl", help="Path to input JSONL file")
    parser.add_argument("--output", default="arctic_out/comments.csv", help="Path to output CSV file")
    args = parser.parse_args()

    jsonl_to_csv(args.input, args.output)
