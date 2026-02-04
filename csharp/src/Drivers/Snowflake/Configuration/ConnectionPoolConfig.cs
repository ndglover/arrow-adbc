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
using System.ComponentModel.DataAnnotations;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Configuration
{
    /// <summary>
    /// Represents connection pool configuration parameters.
    /// </summary>
    public class ConnectionPoolConfig
    {
        /// <summary>
        /// Gets or sets the maximum number of connections in the pool.
        /// </summary>
        [Range(1, 1000)]
        public int MaxPoolSize { get; set; } = 10;

        /// <summary>
        /// Gets or sets the minimum number of connections to maintain in the pool.
        /// </summary>
        [Range(0, 100)]
        public int MinPoolSize { get; set; } = 0;

        /// <summary>
        /// Gets or sets the maximum time to wait for a connection from the pool.
        /// </summary>
        public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the maximum idle time before a connection is removed from the pool.
        /// </summary>
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Gets or sets the maximum lifetime of a connection in the pool.
        /// </summary>
        public TimeSpan MaxConnectionLifetime { get; set; } = TimeSpan.FromHours(1);

        /// <summary>
        /// Gets or sets whether to validate connections when acquired from the pool.
        /// </summary>
        public bool ValidateOnAcquire { get; set; } = true;

        /// <summary>
        /// Gets or sets the interval for cleaning up idle connections.
        /// </summary>
        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets whether connection pooling is enabled.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the policy for handling pool overflow.
        /// </summary>
        public PoolOverflowPolicy OverflowPolicy { get; set; } = PoolOverflowPolicy.Block;
    }

    /// <summary>
    /// Represents policies for handling connection pool overflow.
    /// </summary>
    public enum PoolOverflowPolicy
    {
        /// <summary>
        /// Block and wait for an available connection.
        /// </summary>
        Block,

        /// <summary>
        /// Throw an exception when the pool is full.
        /// </summary>
        Reject,

        /// <summary>
        /// Create a new connection outside the pool (not recommended).
        /// </summary>
        CreateNew
    }
}