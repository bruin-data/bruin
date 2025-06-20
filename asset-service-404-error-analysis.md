# Asset Service 404/500 Error Analysis

## Problem Summary

Based on the Slack thread context, the Cloud application is experiencing issues where requests to an asset-service that return 404 errors are being converted to 500 errors instead of being properly passed through as 404s.

### Issue Context
- **Alert**: "Too many 500 Errors in Cloud (Bruin Alerts)"
- **Logs show**: `/index.php\" 500` errors in the cloud namespace 
- **Root cause**: Users switching clients in multiple browser tabs causes old tabs to make requests to asset-service for resources from different clients
- **Current behavior**: Asset-service returns 404, but Cloud app converts this to 500
- **Desired behavior**: Asset-service 404s should be passed through as 404s

## Repository Analysis

This repository (`bruin`) contains:
- **Primary component**: Bruin CLI tool written in Go
- **Purpose**: Data pipeline management and transformation tool
- **Architecture**: Command-line interface with various data connectors (BigQuery, Snowflake, etc.)

### Key Findings

1. **Wrong Repository**: This appears to be the CLI tool repository, not the Cloud web application experiencing the 500 errors.

2. **HTTP Error Handling Found**: The repository does contain HTTP error handling patterns, particularly in:
   - `pkg/bigquery/db.go` - Contains 404 error handling for BigQuery API
   - `pkg/bigquery/db_test.go` - Test cases showing proper status code handling

3. **No Asset Service Calls**: No direct references to "asset-service" API calls were found in this codebase.

## Recommended Solution

The fix needs to be implemented in the **Cloud web application** (likely a separate PHP-based repository), not in this CLI tool repository.

### Implementation Steps

1. **Locate the Cloud Application Code**
   - Find the PHP-based web application that makes requests to asset-service
   - This is likely in a separate repository from this CLI tool

2. **Identify Asset Service HTTP Client Code**
   - Look for HTTP client code making requests to asset-service endpoints
   - Find error handling logic that processes asset-service responses

3. **Fix Error Handling Logic**
   - Modify the error handling to preserve HTTP status codes from asset-service
   - Ensure 404 responses from asset-service are returned as 404s, not converted to 500s

### Example Fix Pattern

```php
// Before (problematic)
try {
    $response = $httpClient->get($assetServiceUrl);
    // Process response...
} catch (RequestException $e) {
    // All errors converted to 500
    throw new InternalServerErrorException('Asset service error');
}

// After (correct)
try {
    $response = $httpClient->get($assetServiceUrl);
    // Process response...
} catch (RequestException $e) {
    $statusCode = $e->getResponse() ? $e->getResponse()->getStatusCode() : 500;
    
    if ($statusCode === 404) {
        throw new NotFoundHttpException('Asset not found');
    }
    
    // Only convert other errors to 500
    throw new InternalServerErrorException('Asset service error');
}
```

## Validation

After implementing the fix:

1. **Test the scenario**: Open multiple tabs, switch clients in one tab, verify other tabs return 404s instead of 500s
2. **Monitor Grafana alerts**: Confirm the "Too many 500 Errors in Cloud" alert no longer triggers for this scenario
3. **Check logs**: Verify asset-service 404s are properly logged as 404s, not 500s

## Next Steps

1. **Locate the correct repository** containing the Cloud web application code
2. **Search for asset-service HTTP client implementations** in that codebase
3. **Implement proper error handling** to preserve 404 status codes
4. **Test and deploy** the fix to resolve the alerting issue

---

*Note: This analysis was performed on the Bruin CLI repository. The actual fix needs to be implemented in the separate Cloud web application repository.*