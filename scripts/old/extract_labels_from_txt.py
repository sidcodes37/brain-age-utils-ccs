import os, re, csv

"""
stream_extract_edf_age_gender_txt.py

Stream-extract EDF filename, age, gender from a large plain-text header file.
Hard-coded input/output paths: set INPUT_TXT and OUTPUT_CSV below.
"""

# CONFIGURATION 
INPUT_TXT = "./../TUH-EEG/headers.txt"
OUTPUT_CSV = "./../outputs/TUH-EEG_txt_labels.csv"


# precompile re
RE_EDF = re.compile(r'([A-Za-z0-9_\- ./\\]+\.(?:edf))', re.I)
RE_LPTI_AGE = re.compile(r'lpti[_\s-]*age\s*[:=]?\s*\[?\s*([^\]\r\n]+?)\s*\]?', re.I)
RE_LPTI_GENDER = re.compile(r'lpti[_\s-]*gender\s*[:=]?\s*\[?\s*([^\]\r\n]+?)\s*\]?', re.I)
RE_GENERIC_AGE = re.compile(r'Age[:=]?\s*([0-9]{1,3})', re.I)
RE_AGE_ALT = re.compile(r'([0-9]{1,3})\s*(?:y\b|yrs?\b|years?\b)', re.I)
RE_DIGITS = re.compile(r'([0-9]{1,3})')
RE_GENDER_KEYWORD = re.compile(r'\b(?:gender|sex|lpti[_\s-]*gender|patient[_\s-]*sex)\b', re.I)
RE_SINGLE_MF = re.compile(r'\b([MF])\b', re.I)

def normalize_gender(raw):
    if not raw:
        return None
    v = raw.strip().strip('[](){}:;.,\'"').lower()
    if not v:
        return None
    if v in ('m','male','man','boy'):
        return 'Male'
    if v in ('f','female','woman','girl'):
        return 'Female'
    if v[0] == 'm':
        return 'Male'
    if v[0] == 'f':
        return 'Female'
    return None

def extract_digits_from_text(s):
    if not s:
        return None
    m = RE_DIGITS.search(s)
    if m:
        return m.group(1)
    return None

def parse_line_for_age(line):
    m = RE_LPTI_AGE.search(line)
    if m:
        d = extract_digits_from_text(m.group(1))
        if d:
            return d
    m = RE_GENERIC_AGE.search(line)
    if m:
        return m.group(1)
    m = RE_AGE_ALT.search(line)
    if m:
        return m.group(1)
    return None

def parse_line_for_gender(line):
    m = RE_LPTI_GENDER.search(line)
    if m:
        return normalize_gender(m.group(1))
    if RE_GENDER_KEYWORD.search(line):
        m2 = RE_SINGLE_MF.search(line)
        if m2:
            return normalize_gender(m2.group(1))
    return None

def process_stream_plain(inpath, outpath):
    written = 0
    scanned = 0
    current = None  # {'edf':basename, 'age':str|None, 'gender':str|None}

    try:
        fh = open(inpath, 'r', encoding='utf-8', errors='replace')
    except Exception as e:
        raise SystemExit(f"Cannot open input file: {e}")

    with fh, open(outpath, 'w', newline='', encoding='utf-8') as csvf:
        writer = csv.writer(csvf)
        writer.writerow(['file_name', 'age', 'gender'])

        for raw_line in fh:
            line = raw_line.rstrip('\n\r')

            # find any .edf occurrences on the line
            for m in RE_EDF.finditer(line):
                candidate = m.group(1).strip().strip('"\'')
                edf_basename = os.path.basename(candidate)
                # flush previous record
                if current and current.get('edf'):
                    writer.writerow([current['edf'], current.get('age') or '', current.get('gender') or ''])
                    csvf.flush()
                    written += 1

                # start new record
                current = {'edf': edf_basename, 'age': None, 'gender': None}
                scanned += 1

                # parse tail of same line after match
                tail = line[m.end():]
                if current['age'] is None:
                    a = parse_line_for_age(tail)
                    if a:
                        current['age'] = a
                if current['gender'] is None:
                    g = parse_line_for_gender(tail)
                    if g:
                        current['gender'] = g

            # if inside a record, scan this line for age/gender
            if current:
                if current['age'] is None:
                    a = parse_line_for_age(line)
                    if a:
                        current['age'] = a
                if current['gender'] is None:
                    g = parse_line_for_gender(line)
                    if g:
                        current['gender'] = g

        # flush last record
        if current and current.get('edf'):
            writer.writerow([current['edf'], current.get('age') or '', current.get('gender') or ''])
            csvf.flush()
            written += 1

    return scanned, written

def main():
    if not os.path.isfile(INPUT_TXT):
        raise SystemExit(f"Input file not found: {INPUT_TXT}")
    scanned, written = process_stream_plain(INPUT_TXT, OUTPUT_CSV)
    print(f"Done. Records started: {scanned}. Rows written: {written}. CSV: {os.path.abspath(OUTPUT_CSV)}")

if __name__ == '__main__':
    main()
