file_path = "GDELT.MASTERREDUCEDV2.TXT"

with open(file_path, 'r', encoding='utf-8') as f:
    first_10000_chars = f.read(10000)
    print(first_10000_chars)