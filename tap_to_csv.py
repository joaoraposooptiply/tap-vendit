#!/usr/bin/env python3
"""
Simple script to run tap-vendit and generate CSV files for each stream.
"""

import json
import csv
import sys
import os
import subprocess
import argparse
from collections import defaultdict

def run_tap_to_csv(config_file="config.json", output_dir="./csv_output"):
    """Run tap and convert output to CSV files."""
    
    print("🚀 Running tap-vendit and generating CSV files...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Run the tap
    cmd = ["poetry", "run", "tap-vendit", "--config", config_file]
    
    try:
        print(f"⏳ Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse Singer output
        schemas = {}
        records = defaultdict(list)
        
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            try:
                data = json.loads(line.strip())
                if data['type'] == 'SCHEMA':
                    schemas[data['stream']] = data['schema']
                elif data['type'] == 'RECORD':
                    records[data['stream']].append(data['record'])
            except json.JSONDecodeError:
                continue
        
        # Write CSV files
        for stream, schema in schemas.items():
            if stream in records:
                csv_file = f"{output_dir}/{stream}.csv"
                fieldnames = list(schema['properties'].keys())
                
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for record in records[stream]:
                        row = {field: record.get(field, '') for field in fieldnames}
                        writer.writerow(row)
                
                print(f"✅ Created {csv_file} with {len(records[stream])} records")
        
        print(f"🎉 CSV files created in {output_dir}/")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        print(f"Error output: {e.stderr}")
        return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run tap-vendit and generate CSV files for each stream")
    parser.add_argument("config", nargs='?', default="config.json", help="Path to config file (default: config.json)")
    parser.add_argument("output", nargs='?', default="./csv_output", help="Output directory (default: ./csv_output)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"❌ Config file '{args.config}' not found")
        sys.exit(1)
    
    success = run_tap_to_csv(args.config, args.output)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 