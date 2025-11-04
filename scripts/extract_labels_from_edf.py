import os, sys, csv, string
from datetime import date, datetime

"""
Search TUH EDF files and write CSV with filename, age, gender.

Primary and only source of metadata:
 - raw EDF header bytes (first 256 bytes)
 - patient identification field at bytes 8:88 (80 bytes)
 - recording identification at bytes 88:168 (80 bytes)
"""

# CONFIGURATION 
ROOT_DIR = "./../TUH-EEG"
OUTPUT_CSV = "./../outputs/TUH-EEG_edf_labels.csv"


def is_tuh_filename(fname):
    if not fname.lower().endswith('.edf'):
        return False
    base = fname[:-4]
    parts = base.split('_')
    if len(parts) < 3:
        return False
    s_part = parts[-2].lower()
    t_part = parts[-1].lower()
    if not (s_part.startswith('s') and t_part.startswith('t')):
        return False
    return (s_part[1:].isdigit() and t_part[1:].isdigit())

def read_raw_edf_header(path):
    try:
        with open(path, 'rb') as f:
            raw = f.read(256)
            if len(raw) < 256:
                return None
            return raw
    except Exception:
        return None

def decode_ascii_field(b):
    if not b:
        return ''
    return b.decode('ascii', errors='replace').rstrip().strip()

def try_parse_birthdate_token(token):
    fmts = ("%d-%b-%Y","%d-%b-%y","%d.%m.%Y","%d.%m.%y","%Y-%m-%d","%d/%m/%Y","%d/%m/%y")
    for fmt in fmts:
        try:
            return datetime.strptime(token, fmt).date()
        except Exception:
            pass
    return None

def compute_age_from_birth(birth, refdate=None):
    if not birth:
        return None
    if isinstance(birth, str):
        birth_str = birth.strip()
        parsed = try_parse_birthdate_token(birth_str)
        if parsed:
            birth = parsed
        else:
            # try year-only inside token
            for i in range(len(birth_str)-3):
                part = birth_str[i:i+4]
                if part.isdigit():
                    try:
                        birth = date(int(part), 1, 1)
                        break
                    except Exception:
                        pass
            else:
                return None
    if not isinstance(birth, date):
        return None
    if refdate is None:
        refdate = date.today()
    return refdate.year - birth.year - ((refdate.month, refdate.day) < (birth.month, birth.day))

def parse_age_from_patient_text(s):
    if not s:
        return None
    lower = s.lower()
    idx = lower.find('age:')
    if idx != -1:
        j = idx + len('age:')
        while j < len(s) and not s[j].isdigit():
            j += 1
        start = j
        while j < len(s) and s[j].isdigit():
            j += 1
        if start < j:
            return s[start:j]
    return None

def parse_gender_from_patient_text(s):
    if not s:
        return None
    tokens = s.split()
    if len(tokens) >= 2:
        g = tokens[1].upper()
        if g in ('M','F'):
            return g
        if g in ('MALE','FEMALE'):
            return 'M' if g.startswith('M') else 'F'
    for t in tokens:
        tl = t.lower()
        if tl in ('m','f','male','female'):
            return 'M' if tl.startswith('m') else 'F'
    return None

def find_birthdate_token_in_text(s):
    if not s:
        return None
    trans = str.maketrans({c: ' ' for c in string.punctuation})
    cleaned = s.translate(trans)
    tokens = cleaned.split()
    for t in tokens:
        parsed = try_parse_birthdate_token(t)
        if parsed:
            return parsed
    return None

def extract_age_gender_from_edf(path):
    age = ""
    gender = ""

    raw = read_raw_edf_header(path)
    if raw:
        patient_raw = raw[8:88]
        recording_raw = raw[88:168]
        patient_txt = decode_ascii_field(patient_raw)
        recording_txt = decode_ascii_field(recording_raw)

        # primary extraction from patient field
        a = parse_age_from_patient_text(patient_txt)
        g = parse_gender_from_patient_text(patient_txt)
        if a:
            age = a
        if g:
            gender = g

        # try recording field if missing
        if not age:
            a = parse_age_from_patient_text(recording_txt)
            if a:
                age = a
        if not gender:
            g = parse_gender_from_patient_text(recording_txt)
            if g:
                gender = g

        # if age still missing, attempt compute from birthdate token found in raw fields
        if not age:
            bd = find_birthdate_token_in_text(patient_txt) or find_birthdate_token_in_text(recording_txt)
            if bd:
                comp = compute_age_from_birth(bd)
                if comp is not None:
                    age = str(comp)

    return age or "", gender or ""

def main(root_dir, out_csv):
    results = []
    total_files = 0
    missing = 0

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if is_tuh_filename(fname):
                total_files += 1
                full_path = os.path.join(dirpath, fname)
                age, gender = extract_age_gender_from_edf(full_path)
                if not age and not gender:
                    missing += 1
                results.append((fname, age, gender))

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["filename", "age", "gender"])
        writer.writerows(results)

    print("Done.")
    print("Scanned:", os.path.abspath(root_dir))
    print("Total EDF files matched:", total_files)
    print("Files without age+gender info:", missing)
    print("CSV written to:", os.path.abspath(out_csv))

if __name__ == "__main__":
    if not os.path.isdir(ROOT_DIR):
        sys.exit(f"Error: directory not found → {ROOT_DIR}")
    main(ROOT_DIR, OUTPUT_CSV)
