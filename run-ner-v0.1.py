import os
import csv
import sys
import argparse
import time
import requests
import subprocess
import logging
from pathlib import Path
from typing import Optional, List

REQUEST_DELAY = 0.34  # ~3 requests/sec
_last_request_time = 0.0
USER_AGENT = {"User-Agent": "pmc-downloader/1.0 (+https://example.org)"}


# for logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



## argument parser
def parse_args():
    parser = argparse.ArgumentParser(description="Get the tmVar3 annotations for PMC IDs")
    parser.add_argument("-i", "--input", required=True, help="Path to CSV file with PMCID column")
    parser.add_argument("-o", "--output", required=True, help="Directory to save extracted files")
    parser.add_argument("--ignore-errors", action="store_true", help="Continue on errors")
    parser.add_argument("--tool", choices=["tmVar3", "bionext"], default="tmVar3", help="Annotation tool to use")
    parser.add_argument("--pipenv-dir", default=".", help="Path to the Pipenv project for bionext")
    parser.add_argument("--bionext-path", default=".", help="Path to the bionext main")

    return parser.parse_args()

# common (specify PMCID or PID)
def read_pmcids(csv_path: str) -> List[str]:
    # note that utf-8-sig encoding is important her
    # because otherwise the Byte Order Mark (BOM) is returned
    # which gives you an idea of how to encode the rest of the file. 
    # in this case it is \ufeffPMID, which means the key does 'PMID'
    # does not work as intended with only utf-8 encoding. hence, utf-8-sig!
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        #for row in reader:
        #    print(row)
        return [row.get("PMID", "").strip() for row in reader if row.get("PMID", "").strip()]


def tmVar3_endpoint(pmid: str) -> str:
    return f"https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocxml?pmids={pmid}&full=true"


## delay in requests
def throttle_request() -> None:
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()


def download_from_tmVar3(pmcid: str, output_dir: str, ignore_errors: bool) -> None:
    pmc_folder = os.path.join(output_dir, f"{pmcid}.xml")
    if os.path.exists(pmc_folder):
        logger.info(f"{pmcid}: already exists")
        return
    url = tmVar3_endpoint(pmcid)
    try:
        throttle_request()
        resp = requests.get(url, stream=True, timeout=120, headers=USER_AGENT)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        with open(pmc_folder, "wb") as f2:
            for chunk in resp.iter_content(chunk_size=1024*1024):
                if chunk:
                    f2.write(chunk)
    except Exception as e:
        logger.error(f"{pmcid}: error - {e}")
        if not ignore_errors:
            raise

def run_bionext(pmcid: str, output_dir: str, ignore_errors: bool, pipenv_dir: str, bionextPath: str) -> None:
    #print(bionextPath)
    pmc_file = os.path.join(output_dir, f"{pmcid}.txt")
    bionextTag = os.path.join(output_dir, "tagger")
    bionextExt = os.path.join(output_dir, "extractor")
    bionextLink = os.path.join(output_dir, "linker")
    os.makedirs(bionextTag, exist_ok=True)
    os.makedirs(bionextExt, exist_ok=True)
    os.makedirs(bionextLink, exist_ok=True)
    if os.path.exists(pmc_file):
        logger.info(f"{pmcid}: already exists")
        return
    cmd = ["pipenv", "run", "python", bionextPath, f"PMID:{pmcid}","--tagger.output_folder", bionextTag, "--linker.output_folder", bionextLink, "--extractor.output_folder", bionextExt]
    #print(cmd)
    try:
        result = subprocess.run(cmd, cwd=pipenv_dir, capture_output=True, text=True, timeout=10000)
        result.check_returncode()
        with open(pmc_file, "w", encoding="utf-8") as f:
            f.write(result.stdout)
    except Exception as e:
        logger.error(f"{pmcid}: error - {e}")
        if not ignore_errors:
            raise



if __name__ == "__main__":
    args = parse_args()
    pmcids = read_pmcids(args.input)
    #print(pmcids)
    os.makedirs(args.output, exist_ok=True)
    for pmc in pmcids:
        logger.info(f"Processing {pmc} with {args.tool}")
        #print(pmc)
        if args.tool == "tmVar3":
            #throttle_request()
            download_from_tmVar3(pmc, args.output, args.ignore_errors)
        else:
            run_bionext(pmc, args.output, args.ignore_errors, args.pipenv_dir, args.bionext_path)



