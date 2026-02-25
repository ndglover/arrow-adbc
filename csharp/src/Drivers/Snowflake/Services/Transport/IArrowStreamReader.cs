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

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport
{
    /// <summary>
    /// Processes Arrow format responses from Snowflake.
    /// </summary>
    public interface IArrowStreamReader
    {
        /// <summary>
        /// Reads an Arrow stream and returns an array stream.
        /// </summary>
        /// <param name="arrowStream">The Arrow stream to read.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An Arrow array stream.</returns>
        Task<IArrowArrayStream> ReadStreamAsync(Stream arrowStream, CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads a single record batch from an Arrow stream.
        /// </summary>
        /// <param name="arrowStream">The Arrow stream to read.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A record batch.</returns>
        Task<RecordBatch?> ReadBatchAsync(Stream arrowStream, CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads the schema from an Arrow stream.
        /// </summary>
        /// <param name="arrowStream">The Arrow stream to read.</param>
        /// <returns>The Arrow schema.</returns>
        Schema ReadSchema(Stream arrowStream);
    }
}
