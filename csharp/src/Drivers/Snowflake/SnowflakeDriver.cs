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
using System.Linq;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake
{
    /// <summary>
    /// Native C# Snowflake driver implementation for Apache Arrow ADBC.
    /// </summary>
    public sealed class SnowflakeDriver : AdbcDriver
    {
        /// <summary>
        /// Opens a database connection using the provided parameters.
        /// </summary>
        /// <param name="parameters">The driver-specific parameters.</param>
        /// <returns>An AdbcDatabase instance.</returns>
        /// <exception cref="ArgumentException">Thrown when the parameters are invalid.</exception>
        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            try
            {
                // Convert parameters dictionary to connection string format for parsing
                var connectionString = string.Join(";", parameters.Select(kvp => $"{kvp.Key}={kvp.Value}"));
                var config = ConnectionStringParser.Parse(connectionString);
                return new SnowflakeDatabase(config);
            }
            catch (ArgumentException ex)
            {
                // Re-throw ArgumentExceptions from ConnectionStringParser with consistent message format
                throw new ArgumentException($"Failed to parse connection parameters: {ex.Message}", nameof(parameters), ex);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Failed to parse connection parameters: {ex.Message}", nameof(parameters), ex);
            }
        }
    }
}