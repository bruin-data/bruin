# Test Assets for Exclude Tag Validation

This directory contains test assets to validate the exclude tag functionality in the linting system.

## Naming Convention

### Assets Starting with 'e' - Excluded from Validation
Assets with names starting with 'e' have the `exclude` tag and should be skipped during validation:

- `e.sql` - Excluded asset with no dependencies
- `ee.sql` - Excluded asset with dependency e  
- `en.sql` - Excluded asset that depends on 'e'
- `ene.sql` - Excluded asset that depends on 'en' 

### Assets Starting with 'n' - Not Excluded from Validation
Assets with names starting with 'n' do not have the exclude tag and should be processed during validation:

- `n.sql` - Non-excluded asset with no dependencies
- `ne.sql` - Non-excluded asset with dependency n 
- `nen.sql` - Non-excluded asset that depends on ne
- `nn.sql` - Non-excluded asset with dependency n 

## Dependencies

The asset names indicate their dependencies:
- `en` depends on `e`
- `ene` depends on `en` 
- `nen` depends on `ne`

## Testing Scenarios

This setup allows testing of:
1. Assets with exclude tags are properly skipped
2. Assets without exclude tags are properly validated
3. Dependency relationships between excluded and non-excluded assets
4. The `UniqueAssetTracker` correctly counts excluded assets
5. The `ExcludedAssetNumber` field is properly populated

## Usage

When running linting with `--exclude-tag exclude`, only assets starting with 'n' should be validated, while assets starting with 'e' should be tracked as excluded. 