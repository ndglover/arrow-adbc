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
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Microsoft.Extensions.Logging;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

namespace Apache.Arrow.Adbc.Drivers.Snowflake;

/// <summary>
/// Snowflake database implementation for ADBC.
/// </summary>
public sealed class SnowflakeDatabase : AdbcDatabase
{
    private readonly IReadOnlyDictionary<string, string>? _parameters;
    private readonly IConnectionPoolManager _connectionPool;
    private readonly HttpClient _httpClient;
    private readonly ILoggerFactory? _loggerFactory;
    private bool _disposed;
    private readonly bool _ownsHttpClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="SnowflakeDatabase"/> class.
    /// </summary>
    /// <param name="parameters">The ADBC connection parameters.</param>
    /// <param name="httpClient">option to pass a custom HttpClient</param>
    /// <param name="loggerFactory">option to pass a custom ILoggerFactory</param>
    public SnowflakeDatabase(
        IReadOnlyDictionary<string, string>? parameters = null,
        HttpClient? httpClient = null,
        ILoggerFactory? loggerFactory = null) : this(parameters, httpClient, loggerFactory, null)
    {
    }


    internal SnowflakeDatabase(
        IReadOnlyDictionary<string, string>? parameters,
        HttpClient? httpClient,
        ILoggerFactory? loggerFactory,
        IConnectionPoolManager? connectionPoolManager)
    {
        _parameters = parameters;
        _loggerFactory = loggerFactory;
        _ownsHttpClient = httpClient == null;
        _httpClient  = httpClient ?? new HttpClient();
        _connectionPool = connectionPoolManager ?? CreateDefaultPool(_httpClient);
    }

    private static IConnectionPoolManager CreateDefaultPool(HttpClient httpClient)
    {
        var authService = new AuthenticationService(
            new BasicAuthenticator(httpClient),
            new KeyPairAuthenticator(httpClient),
            new OAuthAuthenticator(httpClient),
            new SsoAuthenticator(httpClient));

        return new ConnectionPoolManager(authService);
    }

    /// <summary>
    /// Creates a new connection to the Snowflake database.
    /// </summary>
    /// <param name="parameters">Connection-specific parameters that override database parameters.</param>
    /// <returns>An AdbcConnection instance.</returns>
    public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? parameters)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ConnectAsync(parameters).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Asynchronously create a new connection to the Snowflake database.
    /// </summary>
    public async Task<AdbcConnection> ConnectAsync(IReadOnlyDictionary<string, string>? parameters)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var config = ConnectionStringParser.ParseParameters(parameters, _parameters);
        return await SnowflakeConnection.CreateAsync(config, _httpClient, _connectionPool, _loggerFactory).ConfigureAwait(false);
    }


    /// <summary>
    /// Disposes the database and releases any resources.
    /// </summary>
    public override void Dispose()
    {
        if (!_disposed)
        {
            _connectionPool?.Dispose();
            if (_ownsHttpClient)
                _httpClient?.Dispose();
            _disposed = true;
        }
        base.Dispose();
    }
}
