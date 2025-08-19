import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from xml.dom import minidom
import argparse
import json
import re
from pathlib import Path
from openpyxl import load_workbook


def prettify_xml(elem):
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def reconstruct_table_from_ocr(ocr_data, y_threshold=15):
    try:
        texts = ocr_data['res']['rec_texts']
        boxes = ocr_data['res']['rec_polys']

        if not texts or not np.any(boxes):
            print("OCR data is empty or malformed. Cannot reconstruct table.")
            return pd.DataFrame()

        combined_data = sorted(zip(texts, boxes), key=lambda x: x[1][0][1])

        rows = []
        current_row = []

        for text, box in combined_data:
            current_y = box[0][1]
            if not current_row:
                current_row.append((text, box))
            else:
                last_y = current_row[-1][1][0][1]
                if abs(current_y - last_y) < y_threshold:
                    current_row.append((text, box))
                else:
                    rows.append(sorted(current_row, key=lambda x: x[1][0][0]))
                    current_row = [(text, box)]

        if current_row:
            rows.append(sorted(current_row, key=lambda x: x[1][0][0]))

        if not rows:
            return pd.DataFrame()

        header_texts = [text.replace(' ', '_') for text, _ in rows[0]]
        data_rows = [[text for text, _ in row] for row in rows[1:]]

        processed_data = []
        for row in data_rows:
            if len(row) == len(header_texts):
                processed_data.append(row)
            else:
                print(f"Skipping malformed row: {row}")
                continue

        return pd.DataFrame(processed_data, columns=header_texts)

    except (KeyError, IndexError, TypeError) as e:
        print(f"Error processing OCR data: {e}. Check the dictionary structure.")
        return pd.DataFrame()


def get_bold_rows_from_excel(file_path, sheet_name, max_rows=3):
    wb = load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]

    all_bold_rows = []
    last_bold_headers = None

    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows), start=1):
        bold_row = []
        has_bold = False
        for cell in row:
            if cell.value is not None:
                if hasattr(cell.font, "bold") and cell.font.bold:
                    bold_row.append(str(cell.value).strip())
                    has_bold = True
                else:
                    bold_row.append(None)
            else:
                bold_row.append(None)
        if has_bold:
            all_bold_rows.append((row_idx, bold_row))
            last_bold_headers = bold_row
    print(all_bold_rows)
    print(last_bold_headers)
    return last_bold_headers, all_bold_rows


def convert_to_xml(df, root_name, row_name, sheet_name=None, metadata_rows=None):
    if df.empty:
        return None

    if sheet_name:
        root = ET.Element(root_name)
        sheet_element = ET.SubElement(root, 'sheet', name=sheet_name)
        parent_element = sheet_element
    else:
        root = ET.Element(root_name)
        parent_element = root

    if metadata_rows:
        for i, (_, values) in enumerate(metadata_rows):
            text_values = [v for v in values if v]
            if not text_values:
                continue
            tag = "title" if i == 0 else f"subtitle{i}"
            meta_element = ET.SubElement(parent_element, tag)
            meta_element.text = " | ".join(text_values)

    for _, row in df.iterrows():
        entry_element = ET.SubElement(parent_element, row_name)
        for col_name, value in row.items():
            if pd.notna(value):
                clean_col_name = re.sub(r'[:*]', '_', str(col_name))
                clean_col_name = re.sub(r'[^a-zA-Z0-9_]', '', clean_col_name)
                if not clean_col_name or clean_col_name[0].isdigit():
                    clean_col_name = '_' + clean_col_name
                sub_element = ET.SubElement(entry_element, clean_col_name)
                sub_element.text = str(value)

    return root


def convert_data_to_xml_seamless(data, input_type):
    dfs = {}

    try:
        if input_type == 'tsv':
            dfs['mutations'] = (pd.read_csv(data, sep='\t'), None)
        elif input_type == 'excel':
            xls = pd.ExcelFile(data)
            for sheet_name in xls.sheet_names:
                last_bold_headers, all_bold_rows = get_bold_rows_from_excel(data, sheet_name)
                df = pd.read_excel(xls, sheet_name=sheet_name)

                if last_bold_headers:
                    new_cols = []
                    for col_idx, col_name in enumerate(df.columns):
                        if ("Unnamed" in str(col_name)) and last_bold_headers[col_idx]:
                            new_cols.append(last_bold_headers[col_idx])
                        else:
                            new_cols.append(col_name)
                    df.columns = new_cols

                dfs[sheet_name] = (df, all_bold_rows)

        elif input_type == 'ocr':
            with open(data, 'r') as f:
                ocr_dict = json.load(f)
            df_reconstructed = reconstruct_table_from_ocr(ocr_dict)
            if not df_reconstructed.empty:
                dfs['ocr_table'] = (df_reconstructed, None)
        else:
            return f"Error: Unsupported input type '{input_type}'. Please use 'tsv', 'excel', or 'ocr'."
    except FileNotFoundError:
        return f"Error: The file '{data}' was not found."
    except Exception as e:
        return f"An error occurred while reading input data: {e}"

    if not dfs:
        return "Error: No data was successfully processed."

    root_element = ET.Element('bacterial_mutations_data')
    for sheet_name, (df, metadata_rows) in dfs.items():
        if not df.empty:
            xml_element = convert_to_xml(df, 'bacterial_mutations_data', 'mutation_entry', sheet_name=sheet_name, metadata_rows=metadata_rows)
            if xml_element:
                for child in list(xml_element):
                    root_element.append(child)

    return prettify_xml(root_element)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert tabular data from various formats to XML.")
    parser.add_argument('-f', '--file', required=True, help="Path to the input file (e.g., .tsv, .xlsx, or .json for OCR data).")
    parser.add_argument('-t', '--type', choices=['tsv', 'excel', 'ocr'], required=True, help="Format of the input data.")

    args = parser.parse_args()
    xml_output = convert_data_to_xml_seamless(args.file, args.type)
    #print(xml_output)

