# Snowflake ADBC Driver Tests

This directory contains tests for the native C# Snowflake ADBC driver implementation.

## Test Types

### Unit Tests
- `SnowflakeDriverTests.cs` - Tests driver initialization and parameter validation
- `Configuration/ConnectionStringParserTests.cs` - Tests connection string parsing

These tests don't require a Snowflake connection and run automatically.

### Integration Tests
- `ClientTests.cs` - Tests actual Snowflake connectivity and query execution

Integration tests require a real Snowflake instance and credentials.

## Running Integration Tests

Integration tests support two configuration methods:

### Option 1: Environment Variables (Recommended)

Set individual environment variables for your Snowflake credentials:

**Windows (PowerShell):**
```powershell
$env:SNOWFLAKE_ACCOUNT = "your-account"
$env:SNOWFLAKE_USER = "your-username"
$env:SNOWFLAKE_PASSWORD = "your-password"
$env:SNOWFLAKE_DATABASE = "your-database"
$env:SNOWFLAKE_SCHEMA = "your-schema"
$env:SNOWFLAKE_WAREHOUSE = "your-warehouse"
```

**Windows (Command Prompt):**
```cmd
set SNOWFLAKE_ACCOUNT=your-account
set SNOWFLAKE_USER=your-username
set SNOWFLAKE_PASSWORD=your-password
set SNOWFLAKE_DATABASE=your-database
set SNOWFLAKE_SCHEMA=your-schema
set SNOWFLAKE_WAREHOUSE=your-warehouse
```

**Linux/macOS:**
```bash
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-username
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_DATABASE=your-database
export SNOWFLAKE_SCHEMA=your-schema
export SNOWFLAKE_WAREHOUSE=your-warehouse
```

### Option 2: JSON Configuration File

For compatibility with Interop tests, you can use a JSON configuration file.

**1. Create a JSON configuration file** (e.g., `snowflake_config.json`):

```json
{
  "account": "your-account",
  "user": "your-username",
  "password": "your-password",
  "database": "your-database",
  "schema": "your-schema",
  "warehouse": "your-warehouse",
  "query": "SELECT 1 as TESTCOL",
  "authentication": {
    "auth_snowflake": {
      "user": "your-username",
      "password": "your-password"
    }
  },
  "metadata": {
    "catalog": "your-database",
    "schema": "your-schema",
    "table": "your-table"
  }
}
```

**2. Set the environment variable:**

**Windows (PowerShell):**
```powershell
$env:SNOWFLAKE_TEST_CONFIG_FILE = "C:\path\to\snowflake_config.json"
```

**Windows (Command Prompt):**
```cmd
set SNOWFLAKE_TEST_CONFIG_FILE=C:\path\to\snowflake_config.json
```

**Linux/macOS:**
```bash
export SNOWFLAKE_TEST_CONFIG_FILE=/path/to/snowflake_config.json
```

### Running the tests

```bash
# Run all tests
dotnet test

# Run only unit tests (no Snowflake connection needed)
dotnet test --filter "FullyQualifiedName!~ClientTests"

# Run only integration tests
dotnet test --filter "FullyQualifiedName~ClientTests"
```

## Switching from Interop to Native

The native driver uses the **same JSON configuration format** as the Interop driver. This means:

- You can use the same `snowflake_config.json` file for both drivers
- You can easily switch between implementations without changing test configuration
- Test patterns and assertions are compatible

The only difference is the driver initialization - the native driver doesn't need `driverPath` or `driverEntryPoint` fields.

## Security Notes

- Never commit credentials to source control
- Use environment variables or secure credential management systems
- Consider using OAuth or key pair authentication for production scenarios
- Rotate credentials regularly
