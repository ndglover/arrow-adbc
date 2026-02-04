# Apache Arrow ADBC Native Snowflake Driver for C#

This package provides a native C# implementation of the Apache Arrow ADBC (Arrow Database Connectivity) driver for Snowflake databases. Unlike the existing Interop driver that wraps the Go implementation, this driver is implemented entirely in C# and provides direct connectivity to Snowflake while returning results in Apache Arrow columnar format.

## Key Features

- **Native C# Implementation**: Direct C# implementation without Go interop dependencies
- **Arrow Format Support**: Leverages Snowflake's native Arrow support through their SQL REST API
- **ADBC Compliance**: Full adherence to ADBC API standards and conventions
- **Multiple Authentication Methods**: Support for username/password, RSA key pairs, OAuth 2.0, and SSO
- **Performance Optimized**: Connection pooling, streaming, and efficient memory management
- **Comprehensive Type Support**: Handles all Snowflake data types including semi-structured data

## Installation

```bash
dotnet add package Apache.Arrow.Adbc.Drivers.Snowflake
```

## Quick Start

```csharp
using Apache.Arrow.Adbc.Drivers.Snowflake;

// Create driver and database
var driver = new SnowflakeDriver();
var database = driver.Open("account=myaccount;user=myuser;password=mypassword;database=mydb");

// Connect and execute query
using var connection = database.Connect();
using var statement = connection.CreateStatement();
statement.SetSqlQuery("SELECT * FROM my_table LIMIT 10");

// Execute and get results in Arrow format
using var result = statement.ExecuteQuery();
while (result.MoveNext())
{
    var batch = result.Current;
    // Process Arrow RecordBatch
}
```

## Configuration

The driver supports various configuration options through connection strings:

- `account`: Snowflake account identifier
- `user`: Username for authentication
- `password`: Password for basic authentication
- `database`: Default database name
- `schema`: Default schema name
- `warehouse`: Compute warehouse to use
- `role`: Role to assume after connection
- `authenticator`: Authentication method (default, key_pair, oauth, sso)
- `private_key_path`: Path to RSA private key file (for key pair auth)
- `oauth_token`: OAuth access token
- `connection_timeout`: Connection timeout in seconds
- `query_timeout`: Query timeout in seconds

## Authentication Methods

### Username/Password
```csharp
var connectionString = "account=myaccount;user=myuser;password=mypassword";
```

### RSA Key Pair
```csharp
var connectionString = "account=myaccount;user=myuser;authenticator=key_pair;private_key_path=/path/to/key.pem";
```

### OAuth 2.0
```csharp
var connectionString = "account=myaccount;user=myuser;authenticator=oauth;oauth_token=your_token";
```

## Requirements

- .NET 8.0+
- Apache Arrow C# library
- Network connectivity to Snowflake

## License

Licensed under the Apache License, Version 2.0.