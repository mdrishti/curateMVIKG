import csv
import os
import sys
import ftplib
import tarfile
import io
import argparse
import time
import requests
import xml.etree.ElementTree as ET

FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
REQUEST_DELAY = 0.34  # ~3 requests/sec
_last_request_time = 0

def throttle_request():
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description="Download PMC OA archives via FTP using PMCIDs from CSV.")
    parser.add_argument("-i", "--input", required=True, help="Path to CSV file with PMCID column")
    parser.add_argument("-o", "--output", required=True, help="Directory to save extracted files")
    parser.add_argument("--only-xml", action="store_true", help="Download only .nxml files")
    parser.add_argument("--ignore-errors", action="store_true", help="Continue on errors")
    return parser.parse_args()

def read_pmcids(csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row.get("PMCID", "").strip() for row in reader if row.get("PMCID", "").strip()]

def get_ftp_path_from_oa(pmcid):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
    throttle_request()
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    error_elem = root.find("error")
    if error_elem is not None:
        return f"ERROR: {error_elem.attrib.get('code', '')} - {error_elem.text.strip()}"
    for link in root.findall(".//link"):
        if link.attrib.get("format") == "tgz" and link.attrib.get("href", "").startswith("ftp://"):
            return "/".join(link.attrib["href"].split("/pub/pmc/")[1:])
    return None

def files_to_extract(tar, pmcid, only_xml):
    for member in tar.getmembers():
        if only_xml and not member.name.lower().endswith(".nxml"):
            continue
        parts = member.name.split('/')
        parts[0] = pmcid
        member.name = os.path.join(*parts)
        yield member

def download_and_extract(pmcid, archive_path, output_dir, only_xml, ignore_errors):
    if archive_path.startswith("ERROR:"):
        print(f"{pmcid}: {archive_path}")
        return
    pmc_folder = os.path.join(output_dir, pmcid)
    if os.path.exists(pmc_folder):
        print(f"Skipping {pmcid}, already exists")
        return
    os.makedirs(pmc_folder, exist_ok=True)

    for attempt in range(1, 6):
        try:
            with ftplib.FTP(FTP_HOST, timeout=60) as ftp:
                ftp.login()
                ftp.cwd('/pub/pmc')
                with io.BytesIO() as buf:
                    print(f"Downloading {archive_path} (attempt {attempt})...")
                    throttle_request()
                    ftp.retrbinary(f"RETR {archive_path}", buf.write)
                    buf.seek(0)
                    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
                        tar.extractall(path=output_dir, members=files_to_extract(tar, pmcid, only_xml))
            print(f"Extracted to {pmc_folder}")
            return
        except Exception as e:
            print(f"Error downloading {pmcid}: {e}")
            if attempt == 5 and not ignore_errors:
                sys.exit(1)
            time.sleep(3)

def main():
    args = parse_args()
    pmcids = read_pmcids(args.input)
    os.makedirs(args.output, exist_ok=True)

    for pmcid in pmcids:
        archive_path = get_ftp_path_from_oa(pmcid)
        if not archive_path:
            print(f"No archive found for {pmcid}, skipping")
            continue
        download_and_extract(pmcid, archive_path, args.output, args.only_xml, args.ignore_errors)

if __name__ == "__main__":
    main()

