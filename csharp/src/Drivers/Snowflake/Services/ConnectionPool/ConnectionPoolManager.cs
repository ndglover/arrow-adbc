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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

/// <summary>
/// Implements connection pooling for Snowflake connections.
/// </summary>
public class ConnectionPoolManager : IConnectionPoolManager, IDisposable
{
    private readonly IAuthenticationService _authService;
    private readonly ConcurrentDictionary<string, ConnectionPoolEntry> _pools;
    private readonly CancellationTokenSource _cts = new();
    private Task? _cleanupTask;
    private long _totalConnectionsCreated;
    private long _totalConnectionsClosed;
    private long _totalConnectionReuses;
    private int _poolLimitExceeded;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectionPoolManager"/> class.
    /// </summary>
    /// <param name="authService">The authentication service.</param>
    public ConnectionPoolManager(IAuthenticationService authService)
    {
        _authService = authService ?? throw new ArgumentNullException(nameof(authService));
        _pools = new ConcurrentDictionary<string, ConnectionPoolEntry>();
    }

    /// <inheritdoc/>
    public async Task<IPooledConnection> AcquireConnectionAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config));

        EnsureCleanupStarted();

        var poolKey = GeneratePoolKey(config);
        var poolEntry = _pools.GetOrAdd(poolKey, _ => new ConnectionPoolEntry(config));

        SessionCreationToken? token = null;
        var semaphoreAcquired = false;

        try
        {
            await poolEntry.Semaphore.WaitAsync(cancellationToken);
            semaphoreAcquired = true;

            while (poolEntry.IdleConnections.TryPop(out var connection))
            {
                if (connection.Validate())
                {
                    poolEntry.ActiveConnections.Add(connection);
                    Interlocked.Increment(ref _totalConnectionReuses);
                    return connection;
                }

                connection.Dispose();
                Interlocked.Increment(ref _totalConnectionsClosed);
            }

            var currentPoolSize = poolEntry.GetCurrentPoolSize();
            if (currentPoolSize >= config.PoolConfig.MaxPoolSize)
            {
                Interlocked.Increment(ref _poolLimitExceeded);
                throw new InvalidOperationException(
                    $"Connection pool limit reached ({config.PoolConfig.MaxPoolSize}). " +
                    "No connections available.");
            }

            token = poolEntry.TokenCounter.NewToken();
            poolEntry.Semaphore.Release();
            semaphoreAcquired = false;

            var newConnection = await CreateConnectionAsync(config, cancellationToken);

            await poolEntry.Semaphore.WaitAsync(cancellationToken);
            semaphoreAcquired = true;
            poolEntry.TokenCounter.RemoveToken(token);
            poolEntry.ActiveConnections.Add(newConnection);
            Interlocked.Increment(ref _totalConnectionsCreated);

            return newConnection;
        }
        catch
        {
            if (token != null)
            {
                poolEntry.TokenCounter.RemoveToken(token);
            }
            throw;
        }
        finally
        {
            if (semaphoreAcquired)
            {
                poolEntry.Semaphore.Release();
            }
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

        poolEntry.Semaphore.Wait();
        try
        {
            poolEntry.ActiveConnections.Remove(connection);

            var connectionAge = DateTimeOffset.UtcNow - connection.CreatedAt;
            if (connection.IsValid && 
                connectionAge < connection.Config.PoolConfig.MaxConnectionLifetime)
            {
                poolEntry.IdleConnections.Push(connection);
            }
            else
            {
                connection.Dispose();
                Interlocked.Increment(ref _totalConnectionsClosed);
            }
        }
        finally
        {
            poolEntry.Semaphore.Release();
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

        poolEntry.Semaphore.Wait();
        try
        {
            poolEntry.ActiveConnections.Remove(connection);
            connection.Dispose();
            Interlocked.Increment(ref _totalConnectionsClosed);
        }
        finally
        {
            poolEntry.Semaphore.Release();
        }
    }

    /// <inheritdoc/>
    public async Task<PoolStatistics> GetStatisticsAsync()
    {
        var totalConnections = 0;
        var activeConnections = 0;
        var idleConnections = 0;

        foreach (var poolEntry in _pools.Values)
        {
            await poolEntry.Semaphore.WaitAsync();
            try
            {
                totalConnections += poolEntry.GetCurrentPoolSize();
                activeConnections += poolEntry.ActiveConnections.Count;
                idleConnections += poolEntry.IdleConnections.Count;
            }
            finally
            {
                poolEntry.Semaphore.Release();
            }
        }

        return new PoolStatistics
        {
            TotalConnections = totalConnections,
            ActiveConnections = activeConnections,
            IdleConnections = idleConnections,
            PoolLimitExceeded = _poolLimitExceeded,
            TotalConnectionsCreated = _totalConnectionsCreated,
            TotalConnectionsClosed = _totalConnectionsClosed,
            TotalConnectionReuses = _totalConnectionReuses
        };
    }

    /// <summary>
    /// Disposes the connection pool and all pooled connections.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _cts.Cancel();
        _cts.Dispose();

        if (_cleanupTask != null)
        {
            try
            {
                _cleanupTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
            }
        }

        foreach (var poolEntry in _pools.Values)
        {
            poolEntry.Semaphore.Wait();
            try
            {
                foreach (var connection in poolEntry.ActiveConnections)
                {
                    connection.Dispose();
                }

                while (poolEntry.IdleConnections.TryPop(out var connection))
                {
                    connection.Dispose();
                }

                poolEntry.TokenCounter.Reset();
            }
            finally
            {
                poolEntry.Semaphore.Release();
                poolEntry.Semaphore.Dispose();
            }
        }

        _pools.Clear();
    }

    private void EnsureCleanupStarted()
    {
        if (_cleanupTask == null)
        {
            lock (_cts)
            {
                if (_cleanupTask == null)
                {
                    _cleanupTask = CleanupLoopAsync();
                }
            }
        }
    }

    private async Task CleanupLoopAsync()
    {
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(60));
        try
        {
            while (await timer.WaitForNextTickAsync(_cts.Token))
            {
                await CleanupAsync();
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            timer.Dispose();
        }
    }

    private async Task CleanupAsync()
    {
        if (_disposed)
            return;

        foreach (var poolEntry in _pools.Values)
        {
            try
            {
                await poolEntry.Semaphore.WaitAsync(_cts.Token);
                try
                {
                    var now = DateTimeOffset.UtcNow;
                    var connectionsToRemove = new List<IPooledConnection>();

                    while (poolEntry.IdleConnections.TryPop(out var connection))
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
                            poolEntry.IdleConnections.Push(connection);
                        }
                    }

                    foreach (var connection in connectionsToRemove)
                    {
                        connection.Dispose();
                        Interlocked.Increment(ref _totalConnectionsClosed);
                    }
                }
                finally
                {
                    poolEntry.Semaphore.Release();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task<IPooledConnection> CreateConnectionAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken)
    {
        var authToken = await _authService.AuthenticateAsync(
            config.Account,
            config.User,
            config.Authentication,
            config,
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

    private class ConnectionPoolEntry
    {
        public ConnectionConfig Config { get; }
        public HashSet<IPooledConnection> ActiveConnections { get; }
        public ConcurrentStack<IPooledConnection> IdleConnections { get; }
        public SemaphoreSlim Semaphore { get; }
        public SessionCreationTokenCounter TokenCounter { get; }

        public ConnectionPoolEntry(ConnectionConfig config)
        {
            Config = config;
            ActiveConnections = new HashSet<IPooledConnection>();
            IdleConnections = new ConcurrentStack<IPooledConnection>();
            Semaphore = new SemaphoreSlim(1, 1);
            TokenCounter = new SessionCreationTokenCounter(TimeSpan.FromSeconds(60));
        }

        public int GetCurrentPoolSize()
        {
            return ActiveConnections.Count + IdleConnections.Count + TokenCounter.Count();
        }
    }
}
