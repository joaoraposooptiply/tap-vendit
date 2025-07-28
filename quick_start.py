#!/usr/bin/env python3
"""
Quick Start Guide for tap-vendit with Dynamic Schema Discovery

This script demonstrates how to use the tap without any pre-existing catalog files.
All schemas are generated automatically from API responses.
"""

import json
import sys
from pathlib import Path

def create_sample_config():
    """Create a sample config file."""
    config = {
        "vendit_api_key": "your_api_key_here",
        "username": "your_username_here", 
        "password": "your_password_here",
        "api_url": "https://api.staging.vendit.online",
        "start_date": "2024-01-01T00:00:00Z"
    }
    
    with open("config-sample.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print("✅ Created config-sample.json")
    print("📝 Please update it with your actual credentials")

def demonstrate_dynamic_discovery():
    """Demonstrate dynamic schema discovery."""
    print("\n🚀 Dynamic Schema Discovery Demo")
    print("=" * 50)
    
    print("""
The tap now uses dynamic schema discovery, which means:

✅ No catalog files needed - schemas are generated automatically
✅ No schema files needed - types are inferred from API responses  
✅ No manual maintenance - new fields appear automatically
✅ Zero configuration - just provide your API credentials

How it works:
1. Tap connects to your API
2. Fetches sample data from each endpoint
3. Automatically infers JSON schema types
4. Generates catalog with proper metadata
5. Ready to sync data!

""")

def show_usage_examples():
    """Show usage examples."""
    print("📖 Usage Examples")
    print("=" * 50)
    
    print("""
# 1. Generate catalog automatically
python generate_catalog.py --config config.json

# 2. Use tap directly (generates catalog on-the-fly)
tap-vendit --config config.json --discover > catalog.json

# 3. Sync data with generated catalog
tap-vendit --config config.json --catalog catalog.json

# 4. Test all dynamic streams
python test_dynamic_streams.py

# 5. Compare with archived static schemas
python generate_catalog.py --config config.json --compare archive/catalog.json
""")

def show_benefits():
    """Show the benefits of dynamic schemas."""
    print("🎯 Benefits of Dynamic Schema Discovery")
    print("=" * 50)
    
    print("""
Before (Static Schemas):
❌ Had to update 3 files for each new field
❌ Manual schema maintenance required
❌ Risk of schema drift from API
❌ Time-consuming field additions

After (Dynamic Schemas):
✅ Zero manual maintenance required
✅ Automatic field detection
✅ Schema always matches API
✅ Instant field additions
✅ All streams covered automatically
""")

def main():
    """Main function."""
    print("🎉 Welcome to tap-vendit with Dynamic Schema Discovery!")
    print("=" * 60)
    
    # Check if config exists
    if not Path("config.json").exists():
        print("📋 No config.json found. Creating sample config...")
        create_sample_config()
    else:
        print("✅ config.json found")
    
    demonstrate_dynamic_discovery()
    show_benefits()
    show_usage_examples()
    
    print("\n" + "=" * 60)
    print("🎯 You're all set! No catalog files needed.")
    print("💡 Just update config.json with your credentials and run:")
    print("   python generate_catalog.py --config config.json")
    print("=" * 60)

if __name__ == "__main__":
    main() 