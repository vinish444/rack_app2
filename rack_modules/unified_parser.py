# unified_parser.py
import os
import sys
import glob
import re
import pandas as pd
import csv
from collections import defaultdict

# ------------------- File Readers -------------------
def parse_csv_file(file_path):
    try:
        return pd.read_csv(file_path)
    except:
        pass

    for delimiter in ["\t", ";", "|", ":"]:
        try:
            df = pd.read_csv(file_path, sep=delimiter)
            if len(df.columns) > 1:
                return df
        except:
            continue

    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            sample = f.read(4096)
            f.seek(0)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            has_header = sniffer.has_header(sample)
            reader = csv.reader(f, dialect)
            rows = list(reader)

            headers = rows[0] if has_header else [f"col{i}" for i in range(len(rows[0]))]
            data_rows = rows[1:] if has_header else rows

            return pd.DataFrame(data_rows, columns=headers)
    except:
        return None

def parse_excel_file(file_path):
    try:
        return pd.read_excel(file_path)
    except:
        return None

# ------------------- Connection Extractor -------------------
def extract_connections(df):
    df.columns = [col.strip() for col in df.columns]
    connections = defaultdict(dict)

    for _, row in df.iterrows():
        a_name = row.get('DeviceA Name') or row.get('DeviceA Host Name')
        b_name = row.get('DeviceB Name') or row.get('DeviceB Host Name')
        a_port = row.get('DeviceA Port')
        b_port = row.get('DeviceB Port')

        if not all([a_name, a_port, b_name, b_port]):
            continue

        connections[a_name].setdefault('ports', {})[a_port] = f"{b_name}:{b_port}"
        connections[b_name].setdefault('ports', {})[b_port] = f"{a_name}:{a_port}"

    return connections

# ------------------- Rack Topology -------------------
def generate_rack_topology(df):
    df.columns = [col.strip() for col in df.columns]
    topology = {}

    for _, row in df.iterrows():
        for prefix in ['DeviceA', 'DeviceB']:
            building = str(row.get(f'{prefix} Building', '')).strip()
            rack = str(row.get(f'{prefix} Rack', '')).strip()
            ru = str(row.get(f'{prefix} RU', '')).strip()
            name = str(row.get(f'{prefix} Name') or row.get(f'{prefix} Host Name', '')).strip()
            dtype = str(row.get(f'{prefix} Type', '')).strip()
            plat = str(row.get(f'{prefix} RackPlatform', '')).strip()

            if not all([building, rack, ru, name]):
                continue

            rack_id = f"{rack} ({plat})" if plat else rack
            topology.setdefault(building, {}).setdefault('rack', {}).setdefault(rack_id, {})[ru] = f"{name} ({dtype})"

    return topology

# ------------------- Writers -------------------
def write_connections(connections, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, 'w') as f:
        for device, data in sorted(connections.items()):
            f.write(f"{device}\n")
            if 'ports' in data:
                for port, conn in sorted(data['ports'].items()):
                    f.write(f"  {port} -> {conn}\n")
            f.write("\n")
    print(f"Connections written to {path}")

def write_rack_topology(topology, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, 'w') as f:
        for building, rack_data in sorted(topology.items()):
            f.write(f"{building}\n")
            for rack, rus in sorted(rack_data['rack'].items()):
                f.write(f"  {rack}\n")
                for ru, info in sorted(rus.items(), reverse=True):
                    f.write(f"    RU {ru}: {info}\n")
            f.write("\n")
    print(f"Rack topology written to {path}")

def write_summary(counts, output_dir):
    path = os.path.join(output_dir, "processing_summary.txt")
    with open(path, 'w') as f:
        f.write("Cutsheet Processing Summary:\n\n")
        for sheet_type, devices, racks in counts:
            f.write(f"Cutsheet: {sheet_type}\n")
            f.write(f"  - Devices: {devices}\n")
            f.write(f"  - Racks: {racks}\n\n")
    print(f"Processing summary written to {path}")

# ------------------- Main Unified Runner -------------------
def process_file(file_path, output_dir):
    ext = os.path.splitext(file_path)[-1].lower()
    df = parse_excel_file(file_path) if ext == ".xlsx" else parse_csv_file(file_path)

    if df is None:
        print(f"Failed to parse {file_path}")
        return None

    sheet_type = os.path.splitext(os.path.basename(file_path))[0]
    connections = extract_connections(df)
    topology = generate_rack_topology(df)

    write_connections(connections, output_dir, f"{sheet_type}_connections.txt")
    write_rack_topology(topology, output_dir, f"{sheet_type}_rack_topology.txt")

    return sheet_type, len(connections), sum(len(rack) for building in topology.values() for rack in building['rack'].values())

def main(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    files = glob.glob(os.path.join(input_dir, "*.csv")) + glob.glob(os.path.join(input_dir, "*.xlsx"))
    results = []

    for file in files:
        print(f"Processing {file}")
        result = process_file(file, output_dir)
        if result:
            results.append(result)

    write_summary(results, output_dir)

if __name__ == '__main__':
    input_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    output_dir = sys.argv[2] if len(sys.argv) > 2 else './output_combined'
    main(input_dir, output_dir)