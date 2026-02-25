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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool
{
    /// <summary>
    /// Represents a pooled Snowflake connection.
    /// </summary>
    public class PooledConnection : IPooledConnection
    {
        private bool _disposed;
        private DateTimeOffset _lastUsedAt;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledConnection"/> class.
        /// </summary>
        /// <param name="connectionId">The connection ID.</param>
        /// <param name="authToken">The authentication token.</param>
        /// <param name="config">The connection configuration.</param>
        public PooledConnection(
            string connectionId,
            AuthenticationToken authToken,
            ConnectionConfig config)
        {
            ConnectionId = connectionId ?? throw new ArgumentNullException(nameof(connectionId));
            AuthToken = authToken ?? throw new ArgumentNullException(nameof(authToken));
            Config = config ?? throw new ArgumentNullException(nameof(config));
            CreatedAt = DateTimeOffset.UtcNow;
            _lastUsedAt = CreatedAt;
        }

        /// <inheritdoc/>
        public string ConnectionId { get; }

        /// <inheritdoc/>
        public AuthenticationToken AuthToken { get; }

        /// <inheritdoc/>
        public ConnectionConfig Config { get; }

        /// <inheritdoc/>
        public DateTimeOffset CreatedAt { get; }

        /// <inheritdoc/>
        public DateTimeOffset LastUsedAt
        {
            get => _lastUsedAt;
            private set => _lastUsedAt = value;
        }

        /// <inheritdoc/>
        public bool IsValid
        {
            get
            {
                if (_disposed)
                    return false;

                // Check if token is expired
                if (AuthToken.IsExpired)
                    return false;

                // Check if connection has exceeded its lifetime
                var connectionAge = DateTimeOffset.UtcNow - CreatedAt;
                if (connectionAge > Config.PoolConfig.ConnectionLifetime)
                    return false;

                return true;
            }
        }

        /// <inheritdoc/>
        public async Task<bool> ValidateAsync(CancellationToken cancellationToken = default)
        {
            if (!IsValid)
                return false;

            try
            {
                // Update last used time
                LastUsedAt = DateTimeOffset.UtcNow;

                // In a real implementation, we might execute a simple query
                // to verify the connection is still alive (e.g., SELECT 1)
                // For now, we'll just check the token validity
                
                // If token is expiring soon, we could refresh it here
                if (AuthToken.IsExpiringSoon && AuthToken.CanRefresh)
                {
                    // Token refresh would happen here
                    // For now, we'll just return true if it's still valid
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Disposes the connection.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // In a real implementation, we would close the connection
            // and release any resources
            GC.SuppressFinalize(this);
        }
    }
}
