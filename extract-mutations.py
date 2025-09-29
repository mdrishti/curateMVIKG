# file: extract_dna_mutations.py
import bioc
import json
import argparse
import csv
from typing import List, Dict


def extract_mutations_tmVar3(file_path: str) -> List[Dict]:
    # extract mutation annotations with PMID from a BioC XML file (tmVar3 output - type=DNAMutation or type=ProteinMutation)
    results = []

    #with bioc.BioCXMLDocumentReader(file_path) as reader:
    with bioc.biocxml.iterparse(file_path) as reader:
        #collection_info = reader.get_collection_info()
        for document in reader:
            pmid = document.id

            for passage in document.passages:
                for annotation in passage.annotations:
                    typeAnnot = annotation.infons.get("type")
                    if typeAnnot == "DNAMutation" or typeAnnot == "ProteinMutation":
                        for loc in annotation.locations:
                            results.append({
                                "type": typeAnnot,
                                "pmc-id": "PMC" + pmid,
                                "identifier": annotation.infons.get("identifier"),
                                "text": annotation.text,
                                "offset": loc.offset,
                                "length": loc.length
                            })
    return results


def extract_mutations_bionext(file_path: str) -> List[Dict]:
    # extract mutation annotations with PMID from a BioC XML file (BioNExt output - type=SequenceVariant)
    results = []

    # BioNext is usually JSON
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for doc in data.get("documents", []):
        pmid = doc.get("id")
        for passage in doc.get("passages", []):
            for ann in passage.get("annotations", []):
                typeAnnot = ann["infons"].get("type")
                if typeAnnot == "SequenceVariant":
                    for loc in ann.get("locations", []):
                        results.append({
                            "type": typeAnnot,
                            "pmid": pmid,
                            "identifier": ann["infons"].get("identifier", ""),
                            "text": ann.get("text"),
                            "offset": loc.get("offset"),
                            "length": loc.get("length")
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
    parser.add_argument("--format", required=True, choices=["tmVar3", "bionext"],help="Input format: 'tmVar3' (DNA /Protein mutation) or 'bionext' (SequenceVariant)")
    parser.add_argument("--out", help="Optional output CSV file")

    args = parser.parse_args()
    if args.format == "tmVar3":
        mutations = extract_mutations_tmVar3(args.file)
    else:
        mutations = extract_mutations_bionext(args.file)


    if args.out:
        save_to_csv(mutations, args.out)
    else:
        for m in mutations:
            print(m)

