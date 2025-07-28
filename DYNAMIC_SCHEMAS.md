# Dynamic Schema Discovery

This document explains the new dynamic schema discovery feature that eliminates the need to manually maintain schema files and catalog.json.

## The Problem

Previously, adding new fields to a stream required updating **3 files**:
1. `tap_vendit/schemas/product.json` - Static schema definition
2. `catalog.json` - Full catalog with metadata
3. `catalog-selected.json` - Selected catalog with metadata

This was inefficient and error-prone.

## The Solution: Dynamic Schema Discovery

All streams now automatically generate schemas from API responses, eliminating manual maintenance.

### Available Dynamic Streams

- **DynamicProductsStream** - Products with automatic schema generation
- **DynamicSuppliersStream** - Suppliers with automatic schema generation
- **DynamicOrdersStream** - Orders with automatic schema generation
- **DynamicPurchaseOrdersStream** - Purchase Orders with automatic schema generation
- **DynamicSupplierProductsStream** - Supplier Products with automatic schema generation
- **DynamicPurchaseOrdersOptiplyStream** - Purchase Orders Optiply with automatic schema generation
- **DynamicOrdersOptiplyStream** - Orders Optiply with automatic schema generation

### How It Works

1. **Automatic Schema Generation**: When the tap runs discovery, it fetches a sample record from each API endpoint
2. **Type Inference**: The system automatically infers JSON schema types from the actual data
3. **Dynamic Catalog**: Catalog files are generated automatically with proper metadata

### Benefits

✅ **Zero Manual Maintenance**: No need to update schema files when API changes  
✅ **Automatic Field Detection**: New fields are automatically included  
✅ **Type Safety**: Proper JSON schema types inferred from actual data  
✅ **Consistency**: Schema always matches the actual API response  
✅ **All Streams Covered**: Every stream uses dynamic schema generation  

## Usage

### Option 1: Use Dynamic Streams (Default)

All dynamic streams are now enabled by default in `tap.py`:

```python
STREAM_TYPES = [
    # Use dynamic schema generation for all streams
    DynamicProductsStream,
    DynamicSuppliersStream,
    DynamicOrdersStream,
    DynamicPurchaseOrdersStream,
    DynamicPurchaseOrdersOptiplyStream,
    DynamicSupplierProductsStream,
    DynamicOrdersOptiplyStream,
]
```

### Option 2: Generate Catalog Automatically

Use the provided script to generate catalog files:

```bash
# Generate catalog using dynamic discovery
python generate_catalog.py --config config.json --output catalog-dynamic.json

# Compare with existing static catalog
python generate_catalog.py --config config.json --compare catalog.json
```

### Option 3: Manual Discovery

The tap now supports dynamic discovery out of the box:

```bash
# This will use dynamic schemas automatically for all streams
tap-vendit --config config.json --discover > catalog-dynamic.json
```

## Migration Guide

### From Static to Dynamic Schemas

1. **Dynamic Streams Enabled**: All streams now use dynamic schema generation by default
2. **Remove Static Files**: You can delete all schema files in `tap_vendit/schemas/` (optional)
3. **Regenerate Catalogs**: Use the generation script to create new catalog files
4. **Test**: Verify that all fields are properly detected across all streams

### Backward Compatibility

- Static schema files are still supported (just commented out)
- You can switch between static and dynamic streams by uncommenting the static imports
- Existing catalog files continue to work

## Implementation Details

### Schema Generation Process

1. **Sample Data Fetch**: Gets a single record from each API endpoint
2. **Type Inference**: Analyzes each field's value to determine JSON schema type
3. **Schema Construction**: Builds complete JSON schema with proper types
4. **Metadata Generation**: Creates catalog metadata automatically

### Type Inference Rules

- `None` → `["null", "string"]`
- `bool` → `["boolean", "null"]`
- `int` → `["integer", "null"]`
- `float` → `["number", "null"]`
- `str` → `["string", "null"]` (with date-time detection)
- `list` → `["array", "null"]` with inferred item types
- `dict` → `["object", "null"]` with nested properties

### Error Handling

- Falls back to basic schema if sample data fetch fails
- Logs warnings for schema generation issues
- Continues operation even if schema generation fails

## Example: Adding New Fields

### Before (Static Approach)
When the API added `salesPrices` and `availableStock` fields to products, you had to:

1. Update `product.json` schema file
2. Update `catalog.json` with new fields and metadata
3. Update `catalog-selected.json` with new fields and metadata
4. Test changes manually

### After (Dynamic Approach)
New fields are automatically detected across all streams:

```bash
# Run discovery to see new fields in all streams
tap-vendit --config config.json --discover

# New fields automatically appear in the schema for all streams:
# - Products: salesPrices, availableStock, productVats, etc.
# - Orders: any new order fields
# - Suppliers: any new supplier fields
# - Purchase Orders: any new purchase order fields
# - etc.
```

## Stream-Specific Features

### Products Stream
- Automatically detects nested objects like `suppliers`, `salesPrices`, `availableStock`
- Handles complex nested structures with proper type inference

### Suppliers Stream
- Uses GetAllIds endpoint for sample data
- Handles supplier-specific field types

### Orders Stream
- Uses Find → GetWithDetails pattern for sample data
- Detects order-specific nested structures

### Purchase Orders Stream
- Uses Find → GetWithDetails pattern for sample data
- Handles purchase order-specific field types

### Optiply Streams
- Uses unix timestamp endpoints for sample data
- Automatically adds `unix_timestamp` field to schemas
- Handles Optiply-specific data structures

## Future Enhancements

- **Schema Caching**: Cache generated schemas to avoid repeated API calls
- **Schema Evolution**: Track schema changes over time
- **Validation**: Validate generated schemas against known patterns
- **Custom Type Mappings**: Allow custom type inference rules
- **Batch Schema Generation**: Generate schemas for multiple streams in parallel

## Troubleshooting

### Schema Generation Fails

If dynamic schema generation fails:

1. Check API connectivity
2. Verify authentication
3. Check logs for specific error messages
4. Fall back to static schemas if needed

### Missing Fields

If expected fields are missing:

1. Verify the API endpoint returns the expected data
2. Check if the field is present in the sample record
3. Consider increasing the sample size if needed

### Performance Issues

If discovery is slow:

1. The sample data fetch adds minimal overhead per stream
2. Schema is cached after first generation
3. Consider using static schemas for production if needed

### Stream-Specific Issues

- **Products**: Check if nested objects are properly structured
- **Suppliers**: Verify GetAllIds endpoint is accessible
- **Orders/Purchase Orders**: Check Find endpoint filters
- **Optiply Streams**: Verify unix timestamp endpoints are working 