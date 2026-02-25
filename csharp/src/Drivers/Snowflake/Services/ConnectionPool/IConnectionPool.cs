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
    /// Manages connection pooling for Snowflake connections.
    /// </summary>
    public interface IConnectionPool : IDisposable
    {
        /// <summary>
        /// Acquires a connection from the pool.
        /// </summary>
        /// <param name="config">The connection configuration.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A pooled connection.</returns>
        Task<IPooledConnection> AcquireConnectionAsync(
            ConnectionConfig config,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Releases a connection back to the pool.
        /// </summary>
        /// <param name="connection">The connection to release.</param>
        void ReleaseConnection(IPooledConnection connection);

        /// <summary>
        /// Invalidates a connection and removes it from the pool.
        /// </summary>
        /// <param name="connection">The connection to invalidate.</param>
        void InvalidateConnection(IPooledConnection connection);

        /// <summary>
        /// Gets statistics about the connection pool.
        /// </summary>
        /// <returns>Pool statistics.</returns>
        Task<PoolStatistics> GetStatisticsAsync();
    }

    /// <summary>
    /// Represents a pooled connection.
    /// </summary>
    public interface IPooledConnection : IDisposable
    {
        /// <summary>
        /// Gets the connection ID.
        /// </summary>
        string ConnectionId { get; }

        /// <summary>
        /// Gets the authentication token for this connection.
        /// </summary>
        AuthenticationToken AuthToken { get; }

        /// <summary>
        /// Gets the connection configuration.
        /// </summary>
        ConnectionConfig Config { get; }

        /// <summary>
        /// Gets the time when the connection was created.
        /// </summary>
        DateTimeOffset CreatedAt { get; }

        /// <summary>
        /// Gets the time when the connection was last used.
        /// </summary>
        DateTimeOffset LastUsedAt { get; }

        /// <summary>
        /// Gets a value indicating whether the connection is valid.
        /// </summary>
        bool IsValid { get; }

        /// <summary>
        /// Validates the connection health.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>True if the connection is healthy, false otherwise.</returns>
        Task<bool> ValidateAsync(CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// Represents connection pool statistics.
    /// </summary>
    public class PoolStatistics
    {
        /// <summary>
        /// Gets or sets the total number of connections in the pool.
        /// </summary>
        public int TotalConnections { get; set; }

        /// <summary>
        /// Gets or sets the number of active (in-use) connections.
        /// </summary>
        public int ActiveConnections { get; set; }

        /// <summary>
        /// Gets or sets the number of idle connections.
        /// </summary>
        public int IdleConnections { get; set; }

        /// <summary>
        /// Gets or sets the number of connection requests waiting.
        /// </summary>
        public int WaitingRequests { get; set; }

        /// <summary>
        /// Gets or sets the total number of connections created.
        /// </summary>
        public long TotalConnectionsCreated { get; set; }

        /// <summary>
        /// Gets or sets the total number of connections closed.
        /// </summary>
        public long TotalConnectionsClosed { get; set; }

        /// <summary>
        /// Gets or sets the total number of connection reuses.
        /// </summary>
        public long TotalConnectionReuses { get; set; }
    }
}
