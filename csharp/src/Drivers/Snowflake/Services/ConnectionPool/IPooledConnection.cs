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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

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
