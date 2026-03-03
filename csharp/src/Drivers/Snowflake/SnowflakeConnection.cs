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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.TypeConversion;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Snowflake;

/// <summary>
/// Snowflake connection implementation for ADBC.
/// </summary>
public sealed class SnowflakeConnection : AdbcConnection
{
    private readonly ConnectionConfig _config;
    private readonly IConnectionPoolManager _connectionPool;
    private readonly Dictionary<string, string> _options;
    private IPooledConnection? _pooledConnection;
    private IQueryExecutor? _queryExecutor;
    private PreparedStatementManager? _preparedStatementManager;
    private bool _disposed;
    private readonly ILogger<SnowflakeConnection> _logger;    
        
    private SnowflakeConnection(ConnectionConfig config, IConnectionPoolManager connectionPool, Dictionary<string,string> options,
        IPooledConnection pooledConnection, IQueryExecutor queryExecutor, PreparedStatementManager preparedStatementManager,
        ILogger<SnowflakeConnection> logger)
    {
        _config = config;
        _connectionPool = connectionPool;
        _options = options;
        _pooledConnection = pooledConnection;
        _queryExecutor = queryExecutor;
        _preparedStatementManager = preparedStatementManager;
        _logger = logger;
    }

    /// <summary>
    /// Asynchronously creates and initializes a new SnowflakeConnection.
    /// </summary>
    public static async Task<SnowflakeConnection> CreateAsync(ConnectionConfig config, HttpClient httpClient, IConnectionPoolManager connectionPool, ILogger<SnowflakeConnection>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(connectionPool);
        var log = logger ?? NullLogger<SnowflakeConnection>.Instance;

        var options = new Dictionary<string, string>();

        log.LogDebug("Acquiring pooled connection for user {User} account {Account}", config.User, config.Account);
        var pooledConnection = await connectionPool.AcquireConnectionAsync(config).ConfigureAwait(false);
        if (pooledConnection is null)
        {
            throw new InvalidOperationException("Failed to acquire pooled connection");
        }
        log.LogInformation("Acquired pooled connection {ConnectionId}", pooledConnection.ConnectionId);
                
        var apiClient = new RestApiClient(httpClient, config.EnableCompression);
        var typeConverter = new TypeConverter();

        var queryExecutor = new QueryExecutor(apiClient, typeConverter, config.Account);
        var preparedStatementManager = new PreparedStatementManager(apiClient, typeConverter, config.Account);

        return new SnowflakeConnection(config, connectionPool, options, pooledConnection, queryExecutor, preparedStatementManager, log);
    }

    /// <summary>AdbcDatabaseAdbcDatabase
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
        
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        _options[key] = value ?? string.Empty;
    }

    /// <summary>
    /// Gets the supported table types.
    /// </summary>
    /// <returns>An IArrowArrayStream containing the table types.</returns>
    public override IArrowArrayStream GetTableTypes()
    {
        ThrowIfDisposed();
        throw new NotImplementedException("GetTableTypes not yet implemented");
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
        
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        throw new NotImplementedException("GetTableSchema not yet implemented");
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
        throw new NotImplementedException("GetObjects not yet implemented");
    }

    /// <summary>
    /// Commits the current transaction.
    /// </summary>
    public override void Commit()
    {
        ThrowIfDisposed();
        throw new NotImplementedException("Transaction support not yet implemented");
    }

    /// <summary>
    /// Rolls back the current transaction.
    /// </summary>
    public override void Rollback()
    {
        ThrowIfDisposed();
        throw new NotImplementedException("Transaction support not yet implemented");
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
            _logger.LogDebug("Disposing SnowflakeConnection for user {User}", _config.User);
            _disposed = true;
        }
        base.Dispose();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
