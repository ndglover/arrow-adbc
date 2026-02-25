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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool
{
    /// <summary>
    /// Implements connection pooling for Snowflake connections.
    /// </summary>
    public class ConnectionPool : IConnectionPool, IDisposable
    {
        private readonly IAuthenticationService _authService;
        private readonly ConcurrentDictionary<string, ConnectionPoolEntry> _pools;
        private readonly SemaphoreSlim _poolSemaphore;
        private readonly Timer _cleanupTimer;
        private long _totalConnectionsCreated;
        private long _totalConnectionsClosed;
        private long _totalConnectionReuses;
        private int _waitingRequests;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionPool"/> class.
        /// </summary>
        /// <param name="authService">The authentication service.</param>
        public ConnectionPool(IAuthenticationService authService)
        {
            _authService = authService ?? throw new ArgumentNullException(nameof(authService));
            _pools = new ConcurrentDictionary<string, ConnectionPoolEntry>();
            _poolSemaphore = new SemaphoreSlim(1, 1);

            // Start cleanup timer (runs every 60 seconds)
            _cleanupTimer = new Timer(CleanupCallback, null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));
        }

        /// <inheritdoc/>
        public async Task<IPooledConnection> AcquireConnectionAsync(
            ConnectionConfig config,
            CancellationToken cancellationToken = default)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var poolKey = GeneratePoolKey(config);
            var poolEntry = _pools.GetOrAdd(poolKey, _ => new ConnectionPoolEntry(config));

            await _poolSemaphore.WaitAsync(cancellationToken);
            try
            {
                // Try to get an idle connection
                while (poolEntry.IdleConnections.TryDequeue(out var connection))
                {
                    // Validate the connection
                    if (await connection.ValidateAsync(cancellationToken))
                    {
                        poolEntry.ActiveConnections.Add(connection);
                        Interlocked.Increment(ref _totalConnectionReuses);
                        return connection;
                    }

                    // Connection is invalid, dispose it
                    connection.Dispose();
                    Interlocked.Increment(ref _totalConnectionsClosed);
                }

                // Check if we can create a new connection
                var totalConnections = poolEntry.ActiveConnections.Count + poolEntry.IdleConnections.Count;
                if (totalConnections >= config.PoolConfig.MaxPoolSize)
                {
                    // Wait for a connection to become available
                    Interlocked.Increment(ref _waitingRequests);
                    throw new InvalidOperationException(
                        $"Connection pool limit reached ({config.PoolConfig.MaxPoolSize}). " +
                        "No connections available.");
                }

                // Create a new connection
                var newConnection = await CreateConnectionAsync(config, cancellationToken);
                poolEntry.ActiveConnections.Add(newConnection);
                Interlocked.Increment(ref _totalConnectionsCreated);
                
                return newConnection;
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        /// <inheritdoc/>
        public void ReleaseConnection(IPooledConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            var poolKey = GeneratePoolKey(connection.Config);
            if (!_pools.TryGetValue(poolKey, out var poolEntry))
                return;

            _poolSemaphore.Wait();
            try
            {
                poolEntry.ActiveConnections.Remove(connection);

                // Check if connection is still valid and within lifetime
                var connectionAge = DateTimeOffset.UtcNow - connection.CreatedAt;
                if (connection.IsValid && 
                    connectionAge < connection.Config.PoolConfig.MaxConnectionLifetime)
                {
                    poolEntry.IdleConnections.Enqueue(connection);
                }
                else
                {
                    connection.Dispose();
                    Interlocked.Increment(ref _totalConnectionsClosed);
                }
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        /// <inheritdoc/>
        public void InvalidateConnection(IPooledConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            var poolKey = GeneratePoolKey(connection.Config);
            if (!_pools.TryGetValue(poolKey, out var poolEntry))
                return;

            _poolSemaphore.Wait();
            try
            {
                poolEntry.ActiveConnections.Remove(connection);
                connection.Dispose();
                Interlocked.Increment(ref _totalConnectionsClosed);
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        /// <inheritdoc/>
        public Task<PoolStatistics> GetStatisticsAsync()
        {
            _poolSemaphore.Wait();
            try
            {
                var stats = new PoolStatistics
                {
                    TotalConnections = _pools.Values.Sum(p => p.ActiveConnections.Count + p.IdleConnections.Count),
                    ActiveConnections = _pools.Values.Sum(p => p.ActiveConnections.Count),
                    IdleConnections = _pools.Values.Sum(p => p.IdleConnections.Count),
                    WaitingRequests = _waitingRequests,
                    TotalConnectionsCreated = _totalConnectionsCreated,
                    TotalConnectionsClosed = _totalConnectionsClosed,
                    TotalConnectionReuses = _totalConnectionReuses
                };

                return Task.FromResult(stats);
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        /// <summary>
        /// Disposes the connection pool and all pooled connections.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            _cleanupTimer?.Dispose();
            _poolSemaphore?.Wait();
            try
            {
                foreach (var poolEntry in _pools.Values)
                {
                    foreach (var connection in poolEntry.ActiveConnections)
                    {
                        connection.Dispose();
                    }

                    while (poolEntry.IdleConnections.TryDequeue(out var connection))
                    {
                        connection.Dispose();
                    }
                }

                _pools.Clear();
            }
            finally
            {
                _poolSemaphore?.Release();
                _poolSemaphore?.Dispose();
            }
        }

        private async Task<IPooledConnection> CreateConnectionAsync(
            ConnectionConfig config,
            CancellationToken cancellationToken)
        {
            // Authenticate and get token
            var authToken = await _authService.AuthenticateAsync(
                config.Account,
                config.User,
                config.Authentication,
                cancellationToken);

            return new PooledConnection(
                Guid.NewGuid().ToString(),
                authToken,
                config);
        }

        private static string GeneratePoolKey(ConnectionConfig config)
        {
            return $"{config.Account}|{config.User}|{config.Database}|{config.Schema}|{config.Warehouse}|{config.Role}";
        }

        private void CleanupCallback(object? state)
        {
            if (_disposed)
                return;

            _poolSemaphore.Wait();
            try
            {
                var now = DateTimeOffset.UtcNow;

                foreach (var poolEntry in _pools.Values)
                {
                    var connectionsToRemove = new List<IPooledConnection>();

                    // Check idle connections
                    while (poolEntry.IdleConnections.TryDequeue(out var connection))
                    {
                        var idleTime = now - connection.LastUsedAt;
                        var connectionAge = now - connection.CreatedAt;

                        if (idleTime > poolEntry.Config.PoolConfig.IdleTimeout ||
                            connectionAge > poolEntry.Config.PoolConfig.MaxConnectionLifetime ||
                            !connection.IsValid)
                        {
                            connectionsToRemove.Add(connection);
                        }
                        else
                        {
                            // Put it back if still valid
                            poolEntry.IdleConnections.Enqueue(connection);
                        }
                    }

                    // Dispose removed connections
                    foreach (var connection in connectionsToRemove)
                    {
                        connection.Dispose();
                        Interlocked.Increment(ref _totalConnectionsClosed);
                    }
                }
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        private class ConnectionPoolEntry
        {
            public ConnectionConfig Config { get; }
            public HashSet<IPooledConnection> ActiveConnections { get; }
            public ConcurrentQueue<IPooledConnection> IdleConnections { get; }

            public ConnectionPoolEntry(ConnectionConfig config)
            {
                Config = config;
                ActiveConnections = new HashSet<IPooledConnection>();
                IdleConnections = new ConcurrentQueue<IPooledConnection>();
            }
        }
    }
}
