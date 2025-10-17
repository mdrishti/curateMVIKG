# file: compare_mutations.py
import argparse
import pandas as pd
from rapidfuzz import fuzz
from pathlib import Path
import re

# Amino acid mappings
AA_1TO3 = {
    "A": "Ala", "R": "Arg", "N": "Asn", "D": "Asp", "C": "Cys",
    "E": "Glu", "Q": "Gln", "G": "Gly", "H": "His", "I": "Ile",
    "L": "Leu", "K": "Lys", "M": "Met", "F": "Phe", "P": "Pro",
    "S": "Ser", "T": "Thr", "W": "Trp", "Y": "Tyr", "V": "Val"
}
AA_3TO1 = {v: k for k, v in AA_1TO3.items()}

# Nucleotide mappings (uppercase only, lowercase normalized in code)
NT_1TO3 = {
    "A": "Adenine", "C": "Cytosine", "G": "Guanine", "T": "Thymine", "U": "Uracil"
}
NT_3TO1 = {v: k for k, v in NT_1TO3.items()}


def normalize_mutation(text: str) -> str:
    """Normalize mutation text by mapping amino acids and nucleotides."""
    norm = text

    # Normalize nucleotides to uppercase
    norm = re.sub(r"[acgtu]", lambda m: m.group(0).upper(), norm)

    # Replace 3-letter amino acids with 1-letter
    for aa3, aa1 in AA_3TO1.items():
        norm = re.sub(rf"\b{aa3}\b", aa1, norm, flags=re.IGNORECASE)

    # Replace 1-letter amino acids with 3-letter (like K249 → Lys249)
    for aa1, aa3 in AA_1TO3.items():
        norm = re.sub(rf"\b{aa1}(\d+)\b", f"{aa3}\\1", norm)

    # Replace nucleotide 3-letter with 1-letter
    for nt3, nt1 in NT_3TO1.items():
        norm = re.sub(rf"\b{nt3}\b", nt1, norm, flags=re.IGNORECASE)

    # Replace nucleotide 1-letter with 3-letter (A123 → Adenine123)
    for nt1, nt3 in NT_1TO3.items():
        norm = re.sub(rf"\b{nt1}(\d*)\b", f"{nt3}\\1", norm)

    return norm


def load_tmvar_csv(path: str):
    pmid = Path(path).stem.replace(".xml", "").replace(".txt", "")
    df = pd.read_csv(path)
    df["pmid"] = pmid
    df["normalized"] = df["text"].apply(normalize_mutation)
    return df[["pmid", "text", "normalized", "offset"]].astype(str)


def load_bionext_csv(path: str):
    df = pd.read_csv(path)
    df["normalized"] = df["text"].apply(normalize_mutation)
    return df[["pmid", "text", "normalized", "offset"]].astype(str)


def compare_mutations(tmvar_df, bionext_df, threshold=60, offset_tolerance=5):
    tp, fp, fn = [], [], []
    tmvar_records = tmvar_df.to_dict("records")
    bionext_records = bionext_df.to_dict("records")
    used_bionext = set()

    for t in tmvar_records:
        matched = False
        for i, b in enumerate(bionext_records):
            if i in used_bionext:
                continue
            if t["pmid"] != b["pmid"]:
                continue
            # the offsets are not the same - possibly because the 'passage' length is not the same for either tmVar3 or bionext, therefore it does not make sense to put a threshold over it.
            '''try:
                if abs(int(t["offset"]) - int(b["offset"])) > offset_tolerance:
                    continue
            except ValueError:
                pass'''

            score = fuzz.ratio(t["normalized"], b["normalized"])
            print(t["normalized"] + "\t" + b["normalized"] + "\t" + str(score))
            if score >= threshold:
                tp.append((t["pmid"], t["text"], t["normalized"], t["offset"],
                           b["text"], b["normalized"], b["offset"], score))
                used_bionext.add(i)
                matched = True
                break
        if not matched:
            fn.append((t["pmid"], t["text"], t["normalized"], t["offset"]))

    for i, b in enumerate(bionext_records):
        if i not in used_bionext:
            fp.append((b["pmid"], b["text"], b["normalized"], b["offset"]))

    return tp, fp, fn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare tmVar3 vs BioNext mutations with normalization and fuzzy matching")
    parser.add_argument("--tmvar", required=True, help="CSV file from tmVar3 (filename must include pmid)")
    parser.add_argument("--bionext", required=True, help="CSV file from BioNext")
    parser.add_argument("--threshold", type=int, default=85, help="Fuzzy match threshold")
    parser.add_argument("--out", help="Optional output prefix for TP/FP/FN CSVs")
    args = parser.parse_args()

    tmvar_df = load_tmvar_csv(args.tmvar)
    bionext_df = load_bionext_csv(args.bionext)

    tp, fp, fn = compare_mutations(tmvar_df, bionext_df, args.threshold)

    print(f"True positives: {len(tp)}")
    print(f"False positives (BioNext only): {len(fp)}")
    print(f"False negatives (tmVar only): {len(fn)}")

    if args.out:
        pd.DataFrame(tp, columns=["pmid", "tmvar_text", "tmvar_normalized", "tmvar_offset",
                                  "bionext_text", "bionext_normalized", "bionext_offset", "similarity"]).to_csv(f"{args.out}_TP.csv", index=False)
        pd.DataFrame(fp, columns=["pmid", "bionext_text", "bionext_normalized", "bionext_offset"]).to_csv(f"{args.out}_FP.csv", index=False)
        pd.DataFrame(fn, columns=["pmid", "tmvar_text", "tmvar_normalized", "tmvar_offset"]).to_csv(f"{args.out}_FN.csv", index=False)
        print(f"Results saved with prefix: {args.out}")

