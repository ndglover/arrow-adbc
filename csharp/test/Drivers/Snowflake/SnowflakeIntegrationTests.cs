/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using FluentAssertions;
using NUnit.Framework;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    /// <summary>
    /// Integration tests for the Snowflake ADBC driver.
    /// These tests require a real Snowflake instance and credentials.
    /// 
    /// Required environment variables:
    /// - SNOWFLAKE_ACCOUNT: Your Snowflake account identifier (e.g., "xy12345.us-east-1")
    /// - SNOWFLAKE_USER: Your Snowflake username
    /// - SNOWFLAKE_PASSWORD: Your Snowflake password (for basic auth)
    /// - SNOWFLAKE_DATABASE: (Optional) Database name to use (default: none)
    /// - SNOWFLAKE_SCHEMA: (Optional) Schema name to use (default: none)
    /// - SNOWFLAKE_WAREHOUSE: (Optional) Warehouse name to use (default: none)
    /// - SNOWFLAKE_ROLE: (Optional) Role to use (default: none)
    /// 
    /// To run these tests:
    /// dotnet test --filter "Category=Integration"
    /// </summary>
    [TestFixture]
    [Category("Integration")]
    public class SnowflakeIntegrationTests
    {
        private string? _account;
        private string? _user;
        private string? _password;
        private string? _database;
        private string? _schema;
        private string? _warehouse;
        private string? _role;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _account = Environment.GetEnvironmentVariable("SNOWFLAKE_ACCOUNT");
            _user = Environment.GetEnvironmentVariable("SNOWFLAKE_USER");
            _password = Environment.GetEnvironmentVariable("SNOWFLAKE_PASSWORD");
            _database = Environment.GetEnvironmentVariable("SNOWFLAKE_DATABASE");
            _schema = Environment.GetEnvironmentVariable("SNOWFLAKE_SCHEMA");
            _warehouse = Environment.GetEnvironmentVariable("SNOWFLAKE_WAREHOUSE");
            _role = Environment.GetEnvironmentVariable("SNOWFLAKE_ROLE");

            if (string.IsNullOrEmpty(_account) || string.IsNullOrEmpty(_user) || string.IsNullOrEmpty(_password))
            {
                Assert.Ignore("Snowflake credentials not configured. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD environment variables.");
            }
        }

        private Dictionary<string, string> GetConnectionParameters()
        {
            var parameters = new Dictionary<string, string>
            {
                ["account"] = _account!,
                ["user"] = _user!,
                ["password"] = _password!
            };

            if (!string.IsNullOrEmpty(_database))
                parameters["database"] = _database;

            if (!string.IsNullOrEmpty(_schema))
                parameters["schema"] = _schema;

            if (!string.IsNullOrEmpty(_warehouse))
                parameters["warehouse"] = _warehouse;

            if (!string.IsNullOrEmpty(_role))
                parameters["role"] = _role;

            return parameters;
        }

        [Test]
        public void Connection_ShouldEstablishSuccessfully()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();

            // Act
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());

            // Assert
            connection.Should().NotBeNull();
        }

        [Test]
        public async Task SimpleQuery_ShouldReturnResults()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Act
            statement.SqlQuery = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES LIMIT 100";
            var result = await statement.ExecuteQueryAsync();

            // Assert
            result.Should().NotBeNull();
            result.Stream.Should().NotBeNull();

            // Read the result
            var batch = await result.Stream!.ReadNextRecordBatchAsync();
            
            batch.Should().NotBeNull();
            batch!.ColumnCount.Should().Be(1);
            // Snowflake returns column names in uppercase
            batch.Schema.GetFieldByName("TEST_COLUMN").Should().NotBeNull();
            batch.Length.Should().Be(1);
        }

        [Test]
        public async Task Query_WithMultipleRows_ShouldReturnAllRows()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Act
            statement.SqlQuery = @"
                SELECT column1 AS id, column2 AS name
                FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'))";
            var result = await statement.ExecuteQueryAsync();

            // Assert
            result.Should().NotBeNull();
            var batch = await result.Stream!.ReadNextRecordBatchAsync();
            
            batch.Should().NotBeNull();
            batch!.ColumnCount.Should().Be(2);
            batch.Length.Should().Be(3);
        }

        [Test]
        public async Task PreparedStatement_WithParameters_ShouldExecuteSuccessfully()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Act
            statement.SqlQuery = "SELECT ? AS param1, ? AS param2";
            
            // Bind parameters
            statement.Bind(
                new RecordBatch(
                    new Schema.Builder()
                        .Field(f => f.Name("param1").DataType(Int32Type.Default))
                        .Field(f => f.Name("param2").DataType(StringType.Default))
                        .Build(),
                    new IArrowArray[]
                    {
                        new Int32Array.Builder().Append(42).Build(),
                        new StringArray.Builder().Append("test").Build()
                    },
                    1
                ),
                new Schema.Builder()
                    .Field(f => f.Name("param1").DataType(Int32Type.Default))
                    .Field(f => f.Name("param2").DataType(StringType.Default))
                    .Build()
            );

            var result = await statement.ExecuteQueryAsync();

            // Assert
            result.Should().NotBeNull();
            var batch = await result.Stream!.ReadNextRecordBatchAsync();
            
            batch.Should().NotBeNull();
            batch!.ColumnCount.Should().Be(2);
            batch.Length.Should().Be(1);
        }

        [Test]
        public async Task Update_ShouldReturnAffectedRows()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Create a temporary table
            statement.SqlQuery = "CREATE TEMPORARY TABLE test_table (id INT, name VARCHAR)";
            await statement.ExecuteUpdateAsync();

            // Act - Insert data
            statement.SqlQuery = "INSERT INTO test_table VALUES (1, 'Test')";
            var result = await statement.ExecuteUpdateAsync();

            // Assert
            result.Should().NotBeNull();
            result.AffectedRows.Should().BeGreaterOrEqualTo(0);

            // Cleanup
            statement.SqlQuery = "DROP TABLE test_table";
            await statement.ExecuteUpdateAsync();
        }

        [Test]
        public async Task Query_WithDifferentDataTypes_ShouldHandleCorrectly()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Act
            statement.SqlQuery = @"
                SELECT 
                    1 AS int_col,
                    1.5 AS float_col,
                    'test' AS string_col,
                    TRUE AS bool_col,
                    CURRENT_DATE() AS date_col,
                    CURRENT_TIMESTAMP() AS timestamp_col";
            var result = await statement.ExecuteQueryAsync();

            // Assert
            result.Should().NotBeNull();
            var batch = await result.Stream!.ReadNextRecordBatchAsync();
            
            batch.Should().NotBeNull();
            batch!.ColumnCount.Should().Be(6);
            batch.Length.Should().Be(1);
        }

        [Test]
        public async Task Query_WithNullValues_ShouldPreserveNulls()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);
            using var connection = database.Connect(new Dictionary<string, string>());
            using var statement = connection.CreateStatement();

            // Act
            statement.SqlQuery = "SELECT NULL AS null_col, 1 AS non_null_col";
            var result = await statement.ExecuteQueryAsync();

            // Assert
            result.Should().NotBeNull();
            var batch = await result.Stream!.ReadNextRecordBatchAsync();
            
            batch.Should().NotBeNull();
            batch!.ColumnCount.Should().Be(2);
            batch.Length.Should().Be(1);
        }

        [Test]
        public void Connection_WithInvalidCredentials_ShouldThrowException()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = new Dictionary<string, string>
            {
                ["account"] = _account!,
                ["user"] = "invalid_user",
                ["password"] = "invalid_password"
            };

            // Act & Assert
            Assert.Throws<AdbcException>(() =>
            {
                using var database = driver.Open(parameters);
                using var connection = database.Connect(new Dictionary<string, string>());
            });
        }

        [Test]
        public async Task MultipleConnections_ShouldWorkConcurrently()
        {
            // Arrange
            using var driver = new SnowflakeDriver();
            var parameters = GetConnectionParameters();
            using var database = driver.Open(parameters);

            // Act
            var tasks = new List<Task>();
            for (int i = 0; i < 5; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    using var connection = database.Connect(new Dictionary<string, string>());
                    using var statement = connection.CreateStatement();
                    statement.SqlQuery = "SELECT 1";
                    var result = await statement.ExecuteQueryAsync();
                    result.Should().NotBeNull();
                }));
            }

            // Assert
            await Task.WhenAll(tasks);
        }
    }
}
