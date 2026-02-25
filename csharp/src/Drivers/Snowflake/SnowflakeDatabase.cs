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
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

namespace Apache.Arrow.Adbc.Drivers.Snowflake
{
    /// <summary>
    /// Snowflake database implementation for ADBC.
    /// </summary>
    public sealed class SnowflakeDatabase : AdbcDatabase
    {
        private readonly ConnectionConfig _config;
        private readonly IConnectionPool _connectionPool;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeDatabase"/> class.
        /// </summary>
        /// <param name="config">The connection configuration.</param>
        public SnowflakeDatabase(ConnectionConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            
            // Initialize services
            var httpClient = new HttpClient();
            var basicAuth = new BasicAuthenticator(httpClient);
            var keyPairAuth = new KeyPairAuthenticator(httpClient);
            var oauthAuth = new OAuthAuthenticator(httpClient);
            var ssoAuth = new SsoAuthenticator(httpClient);
            
            var authService = new AuthenticationService(basicAuth, keyPairAuth, oauthAuth, ssoAuth);
            _connectionPool = new ConnectionPool(authService);
        }

        /// <summary>
        /// Creates a new connection to the Snowflake database.
        /// </summary>
        /// <param name="parameters">Connection-specific parameters.</param>
        /// <returns>An AdbcConnection instance.</returns>
        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? parameters)
        {
            ThrowIfDisposed();
            return new SnowflakeConnection(_config, _connectionPool);
        }

        /// <summary>
        /// Disposes the database and releases any resources.
        /// </summary>
        public override void Dispose()
        {
            if (!_disposed)
            {
                _connectionPool?.Dispose();
                _disposed = true;
            }
            base.Dispose();
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SnowflakeDatabase));
            }
        }
    }
}