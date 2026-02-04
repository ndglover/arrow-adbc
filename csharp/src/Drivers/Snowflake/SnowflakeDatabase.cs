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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake
{
    /// <summary>
    /// Snowflake database implementation for ADBC.
    /// </summary>
    public sealed class SnowflakeDatabase : AdbcDatabase
    {
        private readonly ConnectionConfig _config;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeDatabase"/> class.
        /// </summary>
        /// <param name="config">The connection configuration.</param>
        public SnowflakeDatabase(ConnectionConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Creates a new connection to the Snowflake database.
        /// </summary>
        /// <param name="parameters">Connection-specific parameters.</param>
        /// <returns>An AdbcConnection instance.</returns>
        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? parameters)
        {
            ThrowIfDisposed();
            return new SnowflakeConnection(_config);
        }

        /// <summary>
        /// Disposes the database and releases any resources.
        /// </summary>
        public override void Dispose()
        {
            if (!_disposed)
            {
                // TODO: Dispose connection pool and other resources when implemented
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