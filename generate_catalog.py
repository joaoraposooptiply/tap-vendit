#!/usr/bin/env python3
"""
Script to generate catalog files automatically using dynamic schema discovery.
This eliminates the need to manually maintain schema files and catalog.json.
"""

import json
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from tap_vendit.tap import TapVendit

def generate_catalog(config_file: str = None, output_file: str = "catalog-generated.json"):
    """
    Generate a catalog file using dynamic schema discovery.
    
    Args:
        config_file: Path to config file (optional)
        output_file: Output catalog file path
    """
    print("🔍 Generating catalog using dynamic schema discovery...")
    
    # Initialize the tap
    config = None
    if config_file:
        with open(config_file, 'r') as f:
            config = json.load(f)
    
    tap = TapVendit(config=config)
    
    # Discover streams (this will trigger dynamic schema generation)
    streams = tap.discover_streams()
    
    # Build catalog structure
    catalog = {"streams": []}
    
    for stream in streams:
        print(f"📋 Processing stream: {stream.name}")
        
        # Get the dynamically generated schema
        schema = stream.schema
        
        # Build stream metadata
        stream_metadata = []
        
        # Add metadata for each property
        if "properties" in schema:
            for prop_name, prop_schema in schema["properties"].items():
                metadata = {
                    "breadcrumb": ["properties", prop_name],
                    "metadata": {
                        "inclusion": "available"
                    }
                }
                stream_metadata.append(metadata)
        
        # Add stream-level metadata
        stream_metadata.append({
            "breadcrumb": [],
            "metadata": {
                "inclusion": "available",
                "selected": True,
                "selected-by-default": True,
                "table-key-properties": getattr(stream, 'primary_keys', []),
                "valid-replication-keys": [stream.replication_key] if stream.replication_key else []
            }
        })
        
        # Build stream entry
        stream_entry = {
            "tap_stream_id": stream.name,
            "replication_method": "INCREMENTAL" if stream.replication_key else "FULL_TABLE",
            "key_properties": getattr(stream, 'primary_keys', []),
            "schema": schema,
            "stream": stream.name,
            "metadata": stream_metadata
        }
        
        if stream.replication_key:
            stream_entry["replication_key"] = stream.replication_key
        
        catalog["streams"].append(stream_entry)
        print(f"✅ Added stream: {stream.name} with {len(schema.get('properties', {}))} fields")
    
    # Write catalog to file
    with open(output_file, 'w') as f:
        json.dump(catalog, f, indent=2)
    
    print(f"🎉 Catalog generated successfully: {output_file}")
    print(f"📊 Total streams: {len(catalog['streams'])}")
    
    return catalog

def compare_catalogs(static_catalog: str, dynamic_catalog: str):
    """Compare static and dynamic catalogs to show differences."""
    print("\n🔍 Comparing catalogs...")
    
    with open(static_catalog, 'r') as f:
        static = json.load(f)
    
    with open(dynamic_catalog, 'r') as f:
        dynamic = json.load(f)
    
    static_streams = {s["tap_stream_id"]: s for s in static["streams"]}
    dynamic_streams = {s["tap_stream_id"]: s for s in dynamic["streams"]}
    
    for stream_name in dynamic_streams:
        if stream_name in static_streams:
            static_fields = set(static_streams[stream_name]["schema"]["properties"].keys())
            dynamic_fields = set(dynamic_streams[stream_name]["schema"]["properties"].keys())
            
            new_fields = dynamic_fields - static_fields
            if new_fields:
                print(f"🆕 {stream_name}: {len(new_fields)} new fields detected")
                for field in sorted(new_fields):
                    print(f"   + {field}")
        else:
            print(f"🆕 {stream_name}: New stream detected")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate catalog using dynamic schema discovery")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--output", default="catalog-generated.json", help="Output catalog file")
    parser.add_argument("--compare", help="Compare with static catalog file")
    
    args = parser.parse_args()
    
    try:
        catalog = generate_catalog(args.config, args.output)
        
        if args.compare:
            compare_catalogs(args.compare, args.output)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1) 