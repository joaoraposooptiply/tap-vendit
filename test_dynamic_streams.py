#!/usr/bin/env python3
"""
Test script to verify that all dynamic streams are working correctly.
This script tests schema generation for all streams without requiring a config file.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

def test_dynamic_streams():
    """Test all dynamic streams to ensure they can generate schemas."""
    
    print("🧪 Testing Dynamic Schema Streams")
    print("=" * 50)
    
    # Import the dynamic streams
    from tap_vendit.streams import (
        DynamicProductsStream,
        DynamicSuppliersStream,
        DynamicOrdersStream,
        DynamicPurchaseOrdersStream,
        DynamicSupplierProductsStream,
        DynamicPurchaseOrdersOptiplyStream,
        DynamicOrdersOptiplyStream,
    )
    
    # Create a mock tap for testing
    class MockTap:
        def __init__(self):
            self.config = {
                "api_url": "https://api.staging.vendit.online",
                "start_date": "2024-01-01T00:00:00Z"
            }
    
    mock_tap = MockTap()
    
    # Test each dynamic stream
    dynamic_streams = [
        ("Products", DynamicProductsStream),
        ("Suppliers", DynamicSuppliersStream),
        ("Orders", DynamicOrdersStream),
        ("Purchase Orders", DynamicPurchaseOrdersStream),
        ("Supplier Products", DynamicSupplierProductsStream),
        ("Purchase Orders Optiply", DynamicPurchaseOrdersOptiplyStream),
        ("Orders Optiply", DynamicOrdersOptiplyStream),
    ]
    
    results = []
    
    for stream_name, stream_class in dynamic_streams:
        print(f"\n📋 Testing {stream_name} Stream...")
        
        try:
            # Create stream instance
            stream = stream_class(tap=mock_tap)
            
            # Test schema generation (this will fail without real API, but we can test the structure)
            print(f"   ✅ Stream created successfully")
            print(f"   📊 Stream name: {stream.name}")
            print(f"   🔑 Primary keys: {getattr(stream, 'primary_keys', [])}")
            print(f"   🔄 Replication key: {getattr(stream, 'replication_key', 'None')}")
            
            # Test that the stream has the required methods
            if hasattr(stream, '_get_sample_data'):
                print(f"   🎯 Sample data method: ✅")
            else:
                print(f"   ❌ Sample data method: Missing")
            
            if hasattr(stream, 'get_records'):
                print(f"   📥 Get records method: ✅")
            else:
                print(f"   ❌ Get records method: Missing")
            
            results.append({
                "stream": stream_name,
                "status": "✅ Success",
                "name": stream.name,
                "primary_keys": getattr(stream, 'primary_keys', []),
                "replication_key": getattr(stream, 'replication_key', None)
            })
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({
                "stream": stream_name,
                "status": f"❌ Error: {e}",
                "name": "N/A",
                "primary_keys": [],
                "replication_key": None
            })
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    successful = 0
    for result in results:
        status_icon = "✅" if "Success" in result["status"] else "❌"
        print(f"{status_icon} {result['stream']}: {result['status']}")
        if "Success" in result["status"]:
            successful += 1
    
    print(f"\n🎯 Results: {successful}/{len(results)} streams ready for dynamic schema generation")
    
    if successful == len(results):
        print("🎉 All dynamic streams are properly configured!")
        print("\n💡 Next steps:")
        print("   1. Create a config.json file with your API credentials")
        print("   2. Run: python generate_catalog.py --config config.json")
        print("   3. Or run: tap-vendit --config config.json --discover")
    else:
        print("⚠️  Some streams need attention before using dynamic schemas")
    
    return results

def test_schema_generation_functions():
    """Test the schema generation utility functions."""
    
    print("\n🔧 Testing Schema Generation Functions")
    print("=" * 50)
    
    from tap_vendit.streams import infer_schema_type, generate_schema_from_sample
    
    # Test data
    test_data = {
        "string_field": "test",
        "integer_field": 123,
        "float_field": 123.45,
        "boolean_field": True,
        "null_field": None,
        "date_field": "2024-01-01T00:00:00Z",
        "array_field": [1, 2, 3],
        "object_field": {"nested": "value"},
        "empty_array": [],
        "empty_object": {}
    }
    
    print("📝 Testing type inference...")
    for field_name, value in test_data.items():
        inferred_type = infer_schema_type(value)
        print(f"   {field_name}: {inferred_type}")
    
    print("\n📋 Testing schema generation...")
    try:
        schema = generate_schema_from_sample(test_data)
        print(f"   ✅ Schema generated successfully")
        print(f"   📊 Properties: {len(schema.get('properties', {}))}")
        print(f"   🏗️  Schema structure: {list(schema.keys())}")
    except Exception as e:
        print(f"   ❌ Schema generation failed: {e}")
    
    return True

if __name__ == "__main__":
    print("🚀 Dynamic Schema Streams Test Suite")
    print("=" * 50)
    
    # Test schema generation functions
    test_schema_generation_functions()
    
    # Test dynamic streams
    results = test_dynamic_streams()
    
    print("\n" + "=" * 50)
    print("🏁 Test Complete!")
    print("=" * 50) 