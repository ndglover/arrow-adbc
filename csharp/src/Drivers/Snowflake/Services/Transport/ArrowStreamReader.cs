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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport
{
    /// <summary>
    /// Implements Arrow stream reading for Snowflake responses.
    /// </summary>
    public class ArrowStreamReader : IArrowStreamReader
    {
        /// <inheritdoc/>
        public async Task<ArrowArrayStream> ReadStreamAsync(
            Stream arrowStream,
            CancellationToken cancellationToken = default)
        {
            if (arrowStream == null)
                throw new ArgumentNullException(nameof(arrowStream));

            try
            {
                // Read all batches from the stream
                var batches = new List<RecordBatch>();
                Schema? schema = null;

                using (var reader = new ArrowStreamReader(arrowStream, leaveOpen: true))
                {
                    schema = await reader.ReadNextRecordBatchAsync(cancellationToken)
                        .ContinueWith(t => reader.Schema, cancellationToken);

                    RecordBatch? batch;
                    while ((batch = await reader.ReadNextRecordBatchAsync(cancellationToken)) != null)
                    {
                        batches.Add(batch);
                    }
                }

                if (schema == null)
                    throw new InvalidOperationException("Failed to read schema from Arrow stream.");

                // Create an array stream from the batches
                return new InMemoryArrowArrayStream(schema, batches);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to read Arrow stream: {ex.Message}", ex);
            }
        }

        /// <inheritdoc/>
        public async Task<RecordBatch?> ReadBatchAsync(
            Stream arrowStream,
            CancellationToken cancellationToken = default)
        {
            if (arrowStream == null)
                throw new ArgumentNullException(nameof(arrowStream));

            try
            {
                using var reader = new ArrowStreamReader(arrowStream, leaveOpen: true);
                return await reader.ReadNextRecordBatchAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to read Arrow batch: {ex.Message}", ex);
            }
        }

        /// <inheritdoc/>
        public Schema ReadSchema(Stream arrowStream)
        {
            if (arrowStream == null)
                throw new ArgumentNullException(nameof(arrowStream));

            try
            {
                using var reader = new ArrowStreamReader(arrowStream, leaveOpen: true);
                return reader.Schema;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to read Arrow schema: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// In-memory implementation of ArrowArrayStream.
        /// </summary>
        private class InMemoryArrowArrayStream : ArrowArrayStream
        {
            private readonly Schema _schema;
            private readonly List<RecordBatch> _batches;
            private int _currentIndex;

            public InMemoryArrowArrayStream(Schema schema, List<RecordBatch> batches)
            {
                _schema = schema ?? throw new ArgumentNullException(nameof(schema));
                _batches = batches ?? throw new ArgumentNullException(nameof(batches));
                _currentIndex = 0;
            }

            public override Schema Schema => _schema;

            public override ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_currentIndex >= _batches.Count)
                    return new ValueTask<RecordBatch?>((RecordBatch?)null);

                var batch = _batches[_currentIndex];
                _currentIndex++;
                return new ValueTask<RecordBatch?>(batch);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    foreach (var batch in _batches)
                    {
                        batch?.Dispose();
                    }
                    _batches.Clear();
                }
                base.Dispose(disposing);
            }
        }
    }
}
