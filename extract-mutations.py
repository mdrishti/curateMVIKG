# file: extract_dna_mutations.py
import bioc
import argparse
import csv
from typing import List, Dict


def extract_dna_mutations(file_path: str) -> List[Dict]:
    # extract DNA mutation annotations with PMID from a BioC XML file.
    results = []

    #with bioc.BioCXMLDocumentReader(file_path) as reader:
    with bioc.biocxml.iterparse(file_path) as reader:
        collection_info = reader.get_collection_info()
        for document in reader:
            pmid = document.id

            for passage in document.passages:
                for annotation in passage.annotations:
                    if annotation.infons.get("type") == "DNAMutation":
                        for loc in annotation.locations:
                            results.append({
                                "pmid": pmid,
                                "identifier": annotation.infons.get("identifier"),
                                "text": annotation.text,
                                "offset": loc.offset,
                                "length": loc.length
                            })
    return results


def save_to_csv(data: List[Dict], out_path: str) -> None:
    # save extracted mutations to CSV.
    if not data:
        print("No DNA mutations found.")
        return

    fieldnames = list(data[0].keys())
    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract DNA mutations from BioC XML.")
    parser.add_argument("--file", required=True, help="Path to BioC XML file")
    parser.add_argument("--out", help="Optional output CSV file")

    args = parser.parse_args()

    mutations = extract_dna_mutations(args.file)

    if args.out:
        save_to_csv(mutations, args.out)
    else:
        for m in mutations:
            print(m)

