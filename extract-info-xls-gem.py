import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import OrderedDict
import argparse
import json
import re
from pathlib import Path
from openpyxl import load_workbook
import xlrd
import pyexcel
import pandas as pd


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



def search_records(records, phrase):
    phrase = phrase.lower()
    results = []
    for record in records:
        for v in record.values():
            if phrase in str(v).lower():
                results.append(record)
                break  # stop once we found a match in this record
    return results




def convert_data_to_xml_seamless(data, input_type, search_phrase=None):
    #print(data)

    dfs = {}
    
    try:
        if input_type == 'tsv':
            df = pd.read_csv(data, sep='\t')
            if search_phrase:
                phrase = search_phrase.lower()
                df = df[df.apply(lambda row: row.astype(str).str.lower().str.contains(phrase).any(), axis=1)]
                if df.empty:
                    return f"No matches found for '{search_phrase}'."
                return df.to_dict(orient='records')
            dfs['mutations'] = df

        elif input_type == 'excel':
            wb = pyexcel.get_book(file_name=data)
            sheetNames = wb.sheet_names()
            #print(sheetNames)

            for sheet_name in sheetNames:
                #print(sheet_name)
                records = pyexcel.get_records(file_name=data, sheet_name=sheet_name)
 #               if search_phrase:
 #                   phrase = search_phrase.lower()
                if search_phrase:  # normalize: allow a single string OR a list of phrases
                    phrases = ([search_phrase.lower()] if isinstance(search_phrase, str) else [p.lower() for p in search_phrase])
                matches = [
                    row for row in records
                    if any(
                        phrase in str(v).lower()
                        for phrase in phrases
                        for v in row.values()
                    )
                ]
                #matches = [row for row in records if any(phrase in str(v).lower() for v in row.values())]
                if len(matches) >= 1:
                    print(matches)
                    dfs[sheet_name] = matches
                #else:
                #    dfs[sheet_name] = records
            if search_phrase:
                #print(search_phrase)
                if not dfs:
                    return f"No matches found for '{search_phrase}'."
                else:
                    print(f"matches found for '{search_phrase}' in {dfs}")
                return dfs  # return matches directly instead of XML

        elif input_type == 'ocr':
            with open(data, 'r') as f:
                ocr_dict = json.load(f)
            df_reconstructed = reconstruct_table_from_ocr(ocr_dict)

            if not df_reconstructed.empty:
                if search_phrase:
                    phrase = search_phrase.lower()
                    df_reconstructed = df_reconstructed[df_reconstructed.apply(
                        lambda row: row.astype(str).str.lower().str.contains(phrase).any(), axis=1
                    )]
                    if df_reconstructed.empty:
                        return f"No matches found for '{search_phrase}'."
                    return df_reconstructed.to_dict(orient='records')
                dfs['ocr_table'] = df_reconstructed

        else:
            return f"Error: Unsupported input type '{input_type}'."

    except FileNotFoundError:
        return f"Error: The file '{data}' was not found."
    except Exception as e:
        return f"An error occurred while reading input data: {e}"

    if not dfs:
        return "Error: No data was successfully processed."

    # only if no search phrase, we go into XML conversion
    root_element = ET.Element('bacterial_mutations_data')
    for sheet_name, df_data in dfs.items():
        if isinstance(df_data, tuple):
            df, metadata_rows = df_data
        else:
            df, metadata_rows = df_data, None

        # Convert records (list of dicts) into DataFrame for XML converter
        if isinstance(df, list):
            df = pd.DataFrame(df)

        if not df.empty:
            xml_element = convert_to_xml(
                df,
                'bacterial_mutations_data',
                'mutation_entry',
                sheet_name=sheet_name,
                metadata_rows=metadata_rows[:-1] if metadata_rows else None
            )
            if xml_element:
                for child in list(xml_element):
                    root_element.append(child)

    return prettify_xml(root_element)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert tabular data from various formats to XML.")
    parser.add_argument('-f', '--file', required=True, help="Path to the input file (e.g., .tsv, .xlsx, or .json for OCR data).")
    parser.add_argument('-t', '--type', choices=['tsv', 'excel', 'ocr'], required=True, help="Format of the input data.")
    parser.add_argument(
        '-s', '--search', required=False, nargs="+",
        help="Optional phrase to search for in the data. If set, XML is not generated."
    )
    args = parser.parse_args()
    outputAnyKind = convert_data_to_xml_seamless(args.file, args.type, search_phrase=args.search)
    print(outputAnyKind)

