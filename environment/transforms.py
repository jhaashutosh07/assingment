import csv

# Read CSV file into records
def read_csv(filepath):
    records = []
    with open(filepath, newline="") as f:
        for row in csv.DictReader(f):
            records.append(dict(row))
    return {"records": records, "metadata": {"source": filepath}}

# Clean data based on operations
def clean_data(data, operations):
    cleaned = []
    for rec in data["records"]:
        cr = {}
        for k, v in rec.items():
            if "trim_whitespace" in operations and isinstance(v, str):
                v = v.strip()
            if "remove_nulls" in operations and (v is None or v == "" or v == "null"):
                continue
            cr[k] = v
        if cr:
            cleaned.append(cr)
    return {"records": cleaned, "metadata": data.get("metadata", {})}

# Transform data with operations
def transform_data(data, operations):
    records = data["records"]
    for op in operations:
        if op["name"] == "convert_currency":
            col, rate = op["column"], op["rate"]
            for rec in records:
                if col in rec:
                    rec[col] = str(round(float(rec[col]) * rate, 2))
    return {"records": records, "metadata": data.get("metadata", {})}

# Merge multiple datasets
def merge_data(datasets):
    all_records = []
    for idx, ds in enumerate(datasets):
        for rec in ds["records"]:
            e = dict(rec)
            e["_source"] = idx
            all_records.append(e)
    return {"records": all_records, "metadata": {"merged_sources": len(datasets)}}
