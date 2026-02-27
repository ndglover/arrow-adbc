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
        
        // Fast path: try to get idle connection without waiting on semaphore
        while (poolEntry.IdleConnections.TryPop(out var idleConnection))
        {
            if (idleConnection.Validate())
            {
                poolEntry.ActiveConnections.Add(idleConnection);
                Interlocked.Increment(ref _totalConnectionReuses);
                return idleConnection;
            }
            
            // Invalid connection - dispose and release its semaphore slot
            idleConnection.Dispose();
            poolEntry.CapacitySemaphore.Release();
            Interlocked.Increment(ref _totalConnectionsClosed);
        }
        
        // No idle connection available - need to create one
        // Wait for capacity (blocks if pool is full with no idle connections)
        await poolEntry.CapacitySemaphore.WaitAsync(cancellationToken);
        
        try
        {
            // Double-check for idle connection (may have become available while waiting)
            while (poolEntry.IdleConnections.TryPop(out var connection))
            {
                if (connection.Validate())
                {
                    // Found idle connection - release our reserved slot
                    poolEntry.CapacitySemaphore.Release();
                    poolEntry.ActiveConnections.Add(connection);
                    Interlocked.Increment(ref _totalConnectionReuses);
                    return connection;
                }
                
                // Invalid connection - dispose and release its slot
                connection.Dispose();
                poolEntry.CapacitySemaphore.Release();
                Interlocked.Increment(ref _totalConnectionsClosed);
            }
            
            // Create new connection using our reserved slot
            var newConnection = await CreateConnectionAsync(config, cancellationToken);
            poolEntry.AllCreatedConnections.Add(newConnection);
            poolEntry.ActiveConnections.Add(newConnection);
            Interlocked.Increment(ref _totalConnectionsCreated);

            return newConnection;
        }
        catch
        {
            // Release our reserved slot on failure
            poolEntry.CapacitySemaphore.Release();
            throw;
        }
    }
    
    public void ReleaseConnection(IPooledConnection connection)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));

        var poolKey = GeneratePoolKey(connection.Config);
        if (!_pools.TryGetValue(poolKey, out var poolEntry))
            return;

        var connectionAge = DateTimeOffset.UtcNow - connection.CreatedAt;
        if (connection.IsValid && 
            connectionAge < connection.Config.PoolConfig.MaxConnectionLifetime)
        {            
            poolEntry.IdleConnections.Push(connection);
        }
        else
        {            
            connection.Dispose();
            poolEntry.CapacitySemaphore.Release();
            Interlocked.Increment(ref _totalConnectionsClosed);
        }
    }
        
    public void InvalidateConnection(IPooledConnection connection)
    {
        if (connection == null)
            throw new ArgumentNullException(nameof(connection));

        var poolKey = GeneratePoolKey(connection.Config);
        if (!_pools.TryGetValue(poolKey, out var poolEntry))
            return;

        // Dispose connection and release its semaphore slot
        connection.Dispose();
        poolEntry.CapacitySemaphore.Release();
        Interlocked.Increment(ref _totalConnectionsClosed);
    }
        
    public async Task<PoolStatistics> GetStatisticsAsync()
    {
        var totalConnections = 0;
        var activeConnections = 0;
        var idleConnections = 0;

        foreach (var poolEntry in _pools.Values)
        {            
            var poolSize = poolEntry.GetCurrentPoolSize();
            var idle = poolEntry.IdleConnections.Count;
            
            totalConnections += poolSize;
            idleConnections += idle;
            activeConnections += poolSize - idle;
        }

        return new PoolStatistics
        {
            TotalConnections = totalConnections,
            ActiveConnections = activeConnections,
            IdleConnections = idleConnections,
#pragma warning disable CS0618 // Type or member is obsolete
            PoolLimitExceeded = 0, // No longer tracked with new semaphore pattern
#pragma warning restore CS0618 // Type or member is obsolete
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
            foreach (var connection in poolEntry.AllCreatedConnections)
            {
                connection.Dispose();
            }
            
            poolEntry.CapacitySemaphore.Dispose();
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
                    poolEntry.CapacitySemaphore.Release();
                    Interlocked.Increment(ref _totalConnectionsClosed);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        
        await Task.CompletedTask;
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
        public ConcurrentBag<IPooledConnection> ActiveConnections { get; }
        public ConcurrentStack<IPooledConnection> IdleConnections { get; }
        public SemaphoreSlim CapacitySemaphore { get; }
        public ConcurrentBag<IPooledConnection> AllCreatedConnections { get; }

        public ConnectionPoolEntry(ConnectionConfig config)
        {
            Config = config;
            ActiveConnections = new ConcurrentBag<IPooledConnection>();
            IdleConnections = new ConcurrentStack<IPooledConnection>();
            CapacitySemaphore = new SemaphoreSlim(
                config.PoolConfig.MaxPoolSize,
                config.PoolConfig.MaxPoolSize);
            AllCreatedConnections = new ConcurrentBag<IPooledConnection>();
        }

        public int GetCurrentPoolSize()
        {
            return Config.PoolConfig.MaxPoolSize - CapacitySemaphore.CurrentCount;
        }
    }

}
