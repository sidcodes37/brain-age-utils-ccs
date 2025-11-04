import sys
from pathlib import Path

def read_first_256(path):
    with open(path, "rb") as f:
        hdr = f.read(256)
    if len(hdr) < 256:
        raise ValueError("File too small to be a valid EDF (header < 256 bytes)")
    return hdr

def show_patient_field(path):
    hdr = read_first_256(path)
    # bytes 0:8 version, 8:88 patient identification (80 bytes)
    patient_bytes = hdr[8:88]
    recording_bytes = hdr[88:168]

    print("File:", path)
    print("--- raw patient bytes (repr) ---")
    print(repr(patient_bytes))
    print()
    print("--- raw patient bytes (hex) ---")
    print(patient_bytes.hex())
    print()
    
    # decode to ASCII, replace invalid bytes, keep trailing spaces visible with repr
    patient_text = patient_bytes.decode("ascii", errors="replace")
    print("--- patient field decoded (ascii) ---")
    print(repr(patient_text))
    print(patient_text)   # human readable view
    print()
    print("--- recording field decoded (ascii) ---")
    rec_text = recording_bytes.decode("ascii", errors="replace")
    print(repr(rec_text))
    print(rec_text)
    print()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python examine_edf_80_byte_header.py path/to/file.edf")
        raise SystemExit(1)
    p = Path(sys.argv[1])
    if not p.is_file():
        raise SystemExit("File not found: " + str(p))
    show_patient_field(p)
