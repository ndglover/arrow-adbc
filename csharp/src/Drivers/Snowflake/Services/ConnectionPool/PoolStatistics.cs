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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

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
    /// Gets or sets the number of times the pool limit was exceeded.
    /// This property is obsolete and always returns 0 with the semaphore-based pool implementation.
    /// </summary>
    [Obsolete("This property is no longer tracked with the semaphore-based pool implementation and always returns 0.")]
    public int PoolLimitExceeded { get; set; }

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
