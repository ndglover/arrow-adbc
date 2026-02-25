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
using System.Net.Http;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.TypeConversion;

namespace Apache.Arrow.Adbc.Drivers.Snowflake
{
    /// <summary>
    /// Snowflake connection implementation for ADBC.
    /// </summary>
    public sealed class SnowflakeConnection : AdbcConnection
    {
        private readonly ConnectionConfig _config;
        private readonly IConnectionPool _connectionPool;
        private readonly Dictionary<string, string> _options;
        private IPooledConnection? _pooledConnection;
        private IQueryExecutor? _queryExecutor;
        private PreparedStatementManager? _preparedStatementManager;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeConnection"/> class.
        /// </summary>
        /// <param name="config">The connection configuration.</param>
        /// <param name="connectionPool">The connection pool.</param>
        public SnowflakeConnection(ConnectionConfig config, IConnectionPool connectionPool)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
            _options = new Dictionary<string, string>();
            
            // Acquire connection from pool
            _pooledConnection = _connectionPool.AcquireConnectionAsync(_config).GetAwaiter().GetResult();
            
            // Initialize services
            var httpClient = new HttpClient();
            var apiClient = new RestApiClient(httpClient, _config.EnableCompression);
            var streamReader = new SnowflakeArrowStreamReader();
            var typeConverter = new TypeConverter();
            
            _queryExecutor = new QueryExecutor(apiClient, streamReader, typeConverter, _config.Account);
            _preparedStatementManager = new PreparedStatementManager(apiClient, typeConverter, _config.Account);
            
            // Set Arrow format for query results
            InitializeArrowFormatAsync(apiClient).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Creates a new statement for executing queries.
        /// </summary>
        /// <returns>An AdbcStatement instance.</returns>
        public override AdbcStatement CreateStatement()
        {
            ThrowIfDisposed();
            
            if (_pooledConnection == null || _queryExecutor == null || _preparedStatementManager == null)
                throw new InvalidOperationException("Connection is not properly initialized.");
            
            return new SnowflakeStatement(_config, _pooledConnection, _queryExecutor, _preparedStatementManager);
        }

        /// <summary>
        /// Sets a connection option.
        /// </summary>
        /// <param name="key">The option key.</param>
        /// <param name="value">The option value.</param>
        public override void SetOption(string key, string value)
        {
            ThrowIfDisposed();
            
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("Option key cannot be null or empty.", nameof(key));
            }

            _options[key] = value ?? string.Empty;
        }

        /// <summary>
        /// Gets the supported table types.
        /// </summary>
        /// <returns>An IArrowArrayStream containing the table types.</returns>
        public override IArrowArrayStream GetTableTypes()
        {
            ThrowIfDisposed();

            // Snowflake table types: TABLE, VIEW, EXTERNAL TABLE, MATERIALIZED VIEW
            throw new NotImplementedException("Table types retrieval will be implemented in task 9.1");
        }

        /// <summary>
        /// Gets the Arrow schema for a specific table.
        /// </summary>
        /// <param name="catalog">The catalog name (database).</param>
        /// <param name="dbSchema">The schema name.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>The Arrow schema for the table.</returns>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            ThrowIfDisposed();
            
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            }

            throw new NotImplementedException("Table schema retrieval will be implemented in task 9.1");
        }

        /// <summary>
        /// Gets database objects (catalogs, schemas, tables, columns) based on the specified criteria.
        /// </summary>
        /// <param name="depth">The depth of objects to retrieve.</param>
        /// <param name="catalogPattern">The catalog pattern filter.</param>
        /// <param name="dbSchemaPattern">The schema pattern filter.</param>
        /// <param name="tableNamePattern">The table pattern filter.</param>
        /// <param name="tableTypes">The table types to include.</param>
        /// <param name="columnNamePattern">The column pattern filter.</param>
        /// <returns>An IArrowArrayStream containing the database objects.</returns>
        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, 
            string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            ThrowIfDisposed();

            throw new NotImplementedException("Database objects retrieval will be implemented in task 9.1");
        }

        /// <summary>
        /// Commits the current transaction.
        /// </summary>
        public override void Commit()
        {
            ThrowIfDisposed();

            // Snowflake supports transactions via COMMIT statement
            throw new NotImplementedException("Transaction support will be implemented in a later task");
        }

        /// <summary>
        /// Rolls back the current transaction.
        /// </summary>
        public override void Rollback()
        {
            ThrowIfDisposed();

            // Snowflake supports transactions via ROLLBACK statement
            throw new NotImplementedException("Transaction support will be implemented in a later task");
        }

        /// <summary>
        /// Disposes the connection and releases any resources.
        /// </summary>
        public override void Dispose()
        {
            if (!_disposed)
            {
                if (_pooledConnection != null)
                {
                    _connectionPool.ReleaseConnection(_pooledConnection);
                    _pooledConnection = null;
                }
                
                _disposed = true;
            }
            base.Dispose();
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SnowflakeConnection));
            }
        }

        private async Task InitializeArrowFormatAsync(IRestApiClient apiClient)
        {
            try
            {
                // Execute ALTER SESSION command to set Arrow format
                // Use ARROW_FORCE to ensure Arrow format regardless of driver version
                var alterSessionSql = "ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = ARROW_FORCE";
                var queryRequest = RequestBuilder.BuildQueryRequest(
                    alterSessionSql,
                    _config.Database,
                    _config.Schema,
                    _config.Warehouse,
                    _config.Role);

                var accountUrl = _config.Account.Contains(".")
                    ? $"https://{_config.Account}"
                    : $"https://{_config.Account}.snowflakecomputing.com";
                
                // Add query parameters to URL (required by Snowflake v1 API)
                var requestId = Guid.NewGuid().ToString();
                var requestGuid = Guid.NewGuid().ToString();
                var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
                var endpoint = $"{accountUrl}/queries/v1/query-request?requestId={requestId}&request_guid={requestGuid}&startTime={startTime}";
                
                await apiClient.PostAsync<object>(
                    endpoint,
                    queryRequest,
                    _pooledConnection!.AuthToken,
                    default);
            }
            catch (Exception ex)
            {
                // Don't throw - allow connection to proceed even if Arrow format setup fails
                // The queries will still work with JSON format
                Console.WriteLine($"Warning: Failed to initialize Arrow format: {ex.Message}");
            }
        }
    }
}