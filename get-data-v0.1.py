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

REQUEST_DELAY = 0.34  # ~3 requests/sec
_last_request_time = 0.0
USER_AGENT = {"User-Agent": "pmc-downloader/1.0 (+https://example.org)"}

def throttle_request() -> None:
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description="Download PMC ZIP archives via Europe PMC supplementary files API using PMCIDs from CSV.")
    parser.add_argument("-i", "--input", required=True, help="Path to CSV file with PMCID column")
    parser.add_argument("-o", "--output", required=True, help="Directory to save extracted files")
    parser.add_argument("--only-xml", action="store_true", help="Extract only .nxml files")
    parser.add_argument("--ignore-errors", action="store_true", help="Continue on errors")
    return parser.parse_args()

def read_pmcids(csv_path: str) -> List[str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row.get("PMCID", "").strip() for row in reader if row.get("PMCID", "").strip()]

def europepmc_endpoint(pmcid: str) -> str:
    return f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/supplementaryFiles"

def _safe_path(base: str, *paths: str) -> str:
    joined = os.path.normpath(os.path.join(base, *paths))
    if not os.path.commonpath([os.path.abspath(base), joined]) == os.path.abspath(base):
        raise ValueError("Unsafe path detected during extraction")
    return joined

'''def _safe_extract_zip(z: zipfile.ZipFile, dest: str, members: Optional[List[str]] = None) -> None:
    names = members or z.namelist()
    for name in names:
        target = _safe_path(dest, name)
        if name.endswith("/"):
            os.makedirs(target, exist_ok=True)
            continue
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with z.open(name) as src, open(target, "wb") as out:
            out.write(src.read())

def extract_zip_to_pmc_folder(tmp_path: str, pmcid: str, out_dir: str, only_xml: bool) -> None:
    pmc_folder = os.path.join(out_dir, pmcid)
    os.makedirs(pmc_folder, exist_ok=True)
    with zipfile.ZipFile(tmp_path) as z:
        names = [n for n in z.namelist() if (not only_xml or n.lower().endswith(".nxml"))]
        _safe_extract_zip(z, pmc_folder, members=names)
'''
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

# ----------------------------
# (Commented) NCBI OA CGI fallback code kept verbatim for future use
# ----------------------------
# import ftplib
# FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
# def get_ftp_path_from_ncbi(pmcid):
#     url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
#     throttle_request()
#     resp = requests.get(url, timeout=30)
#     resp.raise_for_status()
#     root = ET.fromstring(resp.content)
#     error_elem = root.find("error")
#     if error_elem is not None:
#         print(f"{pmcid}: {error_elem.get('code', '')} - {error_elem.text.strip()}")
#         return None
#     for link in root.findall(".//link"):
#         href = link.attrib.get("href", "")
#         if link.attrib.get("format") == "tgz" and href.endswith(".tar.gz"):
#             return "/".join(href.split("/pub/pmc/")[1:])
#     return None
#
# def download_and_extract_ftp(pmcid, archive_path, output_dir, only_xml, ignore_errors):
#     import io as _io
#     import ftplib as _ftplib
#     pmc_folder = os.path.join(output_dir, pmcid)
#     if os.path.exists(pmc_folder):
#         print(f"Skipping {pmcid}, already exists")
#         return
#     os.makedirs(pmc_folder, exist_ok=True)
#     for attempt in range(1, 6):
#         try:
#             with _ftplib.FTP(FTP_HOST, timeout=60) as ftp:
#                 ftp.login()
#                 ftp.cwd('/pub/pmc')
#                 with _io.BytesIO() as buf:
#                     print(f"Downloading {archive_path} (attempt {attempt})...")
#                     throttle_request()
#                     ftp.retrbinary(f"RETR {archive_path}", buf.write)
#                     buf.seek(0)
#                     with tarfile.open(fileobj=buf, mode="r:gz") as tar:
#                         members = []
#                         for m in tar.getmembers():
#                             if only_xml and not m.name.lower().endswith('.nxml'):
#                                 continue
#                             parts = m.name.split('/')
#                             parts[0] = pmcid
#                             m.name = '/'.join(parts)
#                             members.append(m)
#                         _safe_extract_tar(tar, output_dir, members)
#             print(f"Extracted to {pmc_folder}")
#             return
#         except Exception as e:
#             print(f"Error downloading {pmcid}: {e}")
#             if attempt == 5 and not ignore_errors:
#                 return
#             time.sleep(3)

def main():
    args = parse_args()
    pmcids = read_pmcids(args.input)
    os.makedirs(args.output, exist_ok=True)
    for pmcid in pmcids:
        download_from_europepmc(pmcid, args.output, args.only_xml, args.ignore_errors)

if __name__ == "__main__":
    main()

