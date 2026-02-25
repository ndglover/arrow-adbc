# Snowflake ADBC Driver Integration Tests

This directory contains tests for the Snowflake ADBC driver implementation.

## Test Types

### Unit Tests (`SnowflakeDriverTests.cs`)
These tests run without requiring a real Snowflake connection. They test the driver's basic functionality with mock data.

Run unit tests:
```bash
dotnet test --filter "Category!=Integration"
```

### Integration Tests (`SnowflakeIntegrationTests.cs`)
These tests require a real Snowflake instance and valid credentials. They test the driver against an actual Snowflake database.

## Setting Up Integration Tests

### Prerequisites
1. A Snowflake account with valid credentials
2. Network access to your Snowflake instance

### Environment Variables
Set the following environment variables before running integration tests:

**Required:**
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., "xy12345.us-east-1")
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PASSWORD`: Your Snowflake password

**Optional:**
- `SNOWFLAKE_DATABASE`: Database name to use (default: none)
- `SNOWFLAKE_SCHEMA`: Schema name to use (default: none)
- `SNOWFLAKE_WAREHOUSE`: Warehouse name to use (default: none)
- `SNOWFLAKE_ROLE`: Role to use (default: none)

### Setting Environment Variables

**Windows (PowerShell):**
```powershell
$env:SNOWFLAKE_ACCOUNT = "your-account"
$env:SNOWFLAKE_USER = "your-username"
$env:SNOWFLAKE_PASSWORD = "your-password"
$env:SNOWFLAKE_WAREHOUSE = "your-warehouse"
```

**Windows (Command Prompt):**
```cmd
set SNOWFLAKE_ACCOUNT=your-account
set SNOWFLAKE_USER=your-username
set SNOWFLAKE_PASSWORD=your-password
set SNOWFLAKE_WAREHOUSE=your-warehouse
```

**Linux/macOS:**
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
```

### Running Integration Tests

Run only integration tests:
```bash
dotnet test --filter "Category=Integration"
```

Run all tests (unit + integration):
```bash
dotnet test
```

## Integration Test Coverage

The integration tests cover:

1. **Connection Establishment**: Verifies that the driver can connect to a real Snowflake instance
2. **Simple Queries**: Tests basic SELECT queries with single and multiple rows
3. **Prepared Statements**: Tests parameter binding and prepared statement execution
4. **Data Type Handling**: Verifies correct handling of various Snowflake data types (INT, FLOAT, STRING, BOOL, DATE, TIMESTAMP)
5. **NULL Value Preservation**: Ensures NULL values are correctly preserved in results
6. **Update Operations**: Tests INSERT/UPDATE/DELETE operations and affected row counts
7. **Error Handling**: Verifies proper exception handling for invalid credentials
8. **Concurrent Connections**: Tests connection pooling with multiple concurrent connections

## Troubleshooting

### Tests are skipped
If integration tests are skipped, ensure all required environment variables are set correctly.

### Authentication failures
- Verify your credentials are correct
- Check that your Snowflake account identifier includes the region (e.g., "xy12345.us-east-1")
- Ensure your user has appropriate permissions

### Connection timeouts
- Check network connectivity to Snowflake
- Verify firewall rules allow outbound HTTPS connections
- Ensure your warehouse is running (if specified)

### Query failures
- Verify your user has permissions to execute queries
- If using a specific database/schema, ensure they exist and are accessible
- Check that your warehouse has sufficient resources

## Security Notes

- Never commit credentials to source control
- Use environment variables or secure credential management systems
- Consider using OAuth or key pair authentication for production scenarios
- Rotate credentials regularly
