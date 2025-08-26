import csv
import os
import sys
import zipfile
import argparse
import time
import requests
import xml.etree.ElementTree as ET
from typing import Optional, List
from io import BytesIO
from pathlib import Path

REQUEST_DELAY = 0.34  # ~3 requests/sec
_last_request_time = 0.0
USER_AGENT = {"User-Agent": "pmc-downloader/1.0 (+https://example.org)"}

# log a debugging message
def info(message):
    print(message)
    #print ('{0}> {1}'.format('-'*(2*width+1), message))


## delay in requests
def throttle_request() -> None:
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()


## argument parser
def parse_args():
    parser = argparse.ArgumentParser(description="Download PMC ZIP archives Download PMC OA archives via NCBI FTP or via Europe PMC supplementary files API, using PMCIDs from CSV.")
    parser.add_argument("-i", "--input", required=True, help="Path to CSV file with PMCID column")
    parser.add_argument("-o", "--output", required=True, help="Directory to save extracted files")
    parser.add_argument("-c", "--choice", required=True, type=int, default=1, help="Choose whether file from PMC (1) or EuropePMC (2) should be used")
    parser.add_argument("--path", required=False, type=str, help="Provide the file path of the NCBI ftp if that has to be used")
    parser.add_argument("--only-xml", action="store_true", help="Extract only .nxml files")
    parser.add_argument("--ignore-errors", action="store_true", help="Continue on errors")
    return parser.parse_args()





###################### workflow for EuropePMC API ################################
## read the ftp file with paths (this is default behaviour)
def build_column_mapping(file_path, key_col=2, value_col=0, delimiter="\t", has_header=False):
    """
    build a dictionary mapping from one column to another. args:
        file_path (str): Path to TSV/CSV file.
        key_col (int): Column index (0-based) to use as dictionary key.
        value_col (int): Column index (0-based) to use as dictionary value.
        delimiter (str): File delimiter (default = tab).
        has_header (bool): Whether the first row is a header.

    returns:
        dict: Mapping {key_col_value -> value_col_value}
    """
    mapping = {}
    with open(file_path, newline='', encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        if has_header:
            next(reader, None)  # skip header row
        for row in reader:
            if len(row) > max(key_col, value_col):  # ensure row has enough columns
                key = row[key_col].strip()
                value = row[value_col].strip()
                mapping[key] = value
    return mapping


def europepmc_endpoint(pmcid: str) -> str:
    return f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/supplementaryFiles"


def _safe_path(base: str, *paths: str) -> str:
    joined = os.path.normpath(os.path.join(base, *paths))
    if not os.path.commonpath([os.path.abspath(base), joined]) == os.path.abspath(base):
        raise ValueError("Unsafe path detected during extraction")
    return joined

def _download_zip(content, pmc_id, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"{pmc_id}.zip")
    print(zip_path)
    with open(zip_path, "wb") as f:
        f.write(content)
    try:
        with zipfile.ZipFile(BytesIO(content)) as z:
            z.extractall(os.path.join(out_dir, pmc_id))
    except zipfile.BadZipFile:
        print(f"Downloaded file for {pmc_id} is not a valid ZIP.")


def download_from_europepmc(pmcid: str, output_dir: str, only_xml: bool, ignore_errors: bool) -> None:
    pmc_folder = os.path.join(output_dir, pmcid)
    if os.path.exists(pmc_folder):
        print(f"{pmcid}: already exists")
        return

    url = europepmc_endpoint(pmcid)
    tmp_path = os.path.join(output_dir, f"{pmcid}.tmp")

    try:
        throttle_request()
        resp = requests.get(url, stream=True, timeout=120, headers=USER_AGENT)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")

        '''
        if "zip" in content_type or resp.raw.read(4).startswith(b"PK"):
            resp.close()
            resp = requests.get(url, stream=True, timeout=180, headers=USER_AGENT)
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
            extract_zip_to_pmc_folder(tmp_path, pmcid, output_dir, only_xml)
            os.remove(tmp_path)
            print(f"{pmcid}: downloaded & extracted")
            return
        '''
        if "xml" not in content_type.lower():
            _download_zip(resp.content, pmcid, output_dir)
            return True

        #xml_text = resp.content
        root = ET.fromstring(resp.text)
        err_msg = root.findtext(".//errMsg")
        if err_msg:
            print(f"{pmcid}: {err_msg}")
            return

        links = [e.text.strip() for e in root.iter() if e.text and e.text.strip().lower().endswith(".zip")]
        if not links:
            print(f"{pmcid}: no zip link found")
            return
        archive_url = links[0]

        throttle_request()
        r2 = requests.get(archive_url, stream=True, timeout=180, headers=USER_AGENT)
        r2.raise_for_status()
        with open(tmp_path, "wb") as f2:
            for chunk in r2.iter_content(chunk_size=1024*1024):
                if chunk:
                    f2.write(chunk)
        extract_zip_to_pmc_folder(tmp_path, pmcid, output_dir, only_xml)
        os.remove(tmp_path)
        print(f"{pmcid}: downloaded & extracted")

    except Exception as e:
        print(f"{pmcid}: error - {e}")
        if not ignore_errors:
            raise
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
########### europePMC code ends #############################


########### NCBI OA code ####################################
def read_pmcids(csv_path: str) -> List[str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row.get("PMCID", "").strip() for row in reader if row.get("PMCID", "").strip()]

import ftplib
FTP_HOST = 'ftp.ncbi.nlm.nih.gov'

# following 3 functions taken from https://data.lhncbc.nlm.nih.gov/public/trec-cds-org/download.py
def connect():
  '''Connect to the PMC OAS FTP server'''
  info('Connecting to ftp.ncbi.nlm.nih.gov')
  global pmc
  try:
    pmc = ftplib.FTP(FTP_HOST)
    pmc.login()
    pmc.cwd('/pub/pmc')
  except Exception as e:
    print (e)
    abort(1)
  #heart_beat()

def disconnect():
  '''Disconnect from the PMC OAS FTP server'''
  info('Disconnecting from ftp.ncbi.nlm.nih.gov')
  global pmc
  pmc.close()
  #heart_attack()

def reconnect():
  '''
  Disconnect and then reconnect to the PMC OAS FTP server. This is sometimes
  required because the server can intermittently throw 550 errors, indicating
  that a legitimate file does not exist. When this happens, we reconnect to the
  server and try again.
  '''
  time.sleep(10)
  disconnect()
  time.sleep(10)
  connect()

def get_ftp_path_from_oa(pmcid):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
    throttle_request()
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    error_elem = root.find("error")
    if error_elem is not None:
        print(f"{pmcid}: {error_elem.get('code', '')} - {error_elem.text.strip()}")
        return None
    for link in root.findall(".//link"):
        href = link.attrib.get("href", "")
        if link.attrib.get("format") == "tgz" and href.endswith(".tar.gz"):
            return "/".join(href.split("/pub/pmc/")[1:])
    return None
#
def files_to_extract(tar, pmcid, only_xml):
    for member in tar.getmembers():
        if only_xml and not member.name.lower().endswith(".nxml"):
            continue
        parts = member.name.split('/')
        parts[0] = pmcid
        member.name = os.path.join(*parts)
        yield member


'''
def _safe_extract_tar(tar, output_dir, members=None):
    print(tar)
    for member in tar.getmembers():
        print(member.name)
        extracted_path = output_dir / Path(member.name)
        print(extracted_path)
        if not str(extracted_path.resolve()).startswith(str(output_dir.resolve())):
            raise Exception("Attempted Path Traversal in Tar File")
    tar.extractall(output_dir, members)
'''


def download_and_extract_ftp(pmcid, archive_path, output_dir, only_xml, ignore_errors):
    global pmc
    #print(pmcid)
    '''
    pmc_folder = os.path.join(output_dir, pmcid)
    if os.path.exists(pmc_folder):
        print(f"Skipping {pmcid}, already exists")
        return
    os.makedirs(pmc_folder, exist_ok=True)
    '''
    targz_path = os.path.join(output_dir, '{0}.tar.gz'.format(pmcid))
    if os.path.exists(targz_path):
        print(f"Skipping {pmcid}, already exists")
        return
    #print(archive_path)
    for attempt in range(1, 6):
            try:
                print(f"Downloading {archive_path} (attempt {attempt})...")
                file = open(targz_path, 'wb')
                pmc.retrbinary('RETR %s' % archive_path, file.write)
                file.close()
                print(f"Saved to {targz_path}")
                return True
            except Exception as e:
                print(f"Error downloading {pmcid}: {e}")
                reconnect()
                if attempt == 5 and not ignore_errors:
                    return
                time.sleep(10)

########### NCBI OA code ends ####################################


def main():
    args = parse_args()
    pmcids = read_pmcids(args.input)
    os.makedirs(args.output, exist_ok=True)
    if args.choice == 2:
        for pmcid in pmcids:
            download_from_europepmc(pmcid, args.output, args.only_xml, args.ignore_errors)
    else:
        if (args.path):
            mapping = {}
            mapping = build_column_mapping(args.path, key_col=2, value_col=0)
        connect()
        for pmcid in pmcids:
            throttle_request()
            if (args.path):
                a1 = mapping.get(pmcid)
                #print(pmcid)
                if a1:
                    #print(a1)
                    archive_path = a1
                #    archive_path = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/".join(a1)
                else:
                    archive_path = None
            else:
                archive_path = get_ftp_path_from_oa(pmcid)
            if not archive_path:
                print(f"No archive found for {pmcid}, skipping")
                continue
            download_and_extract_ftp(pmcid, archive_path, args.output, args.only_xml, args.ignore_errors)
        disconnect()

if __name__ == "__main__":
    main()

