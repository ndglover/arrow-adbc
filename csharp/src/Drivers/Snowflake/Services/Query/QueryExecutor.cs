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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Query;

/// <summary>
/// Implements query execution for Snowflake connections.
/// </summary>
public class QueryExecutor : IQueryExecutor
{
    private readonly IRestApiClient _apiClient;
    private readonly string _accountUrl;
    private readonly ILogger<QueryExecutor> _logger;
    private const string QueryEndpoint = "/queries/v1/query-request";

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
    /// </summary>
    /// <param name="apiClient">The REST API client.</param>
    /// <param name="typeConverter">The type converter.</param>
    /// <param name="account">The Snowflake account identifier.</param>
    /// <param name="logger">The ILogger instance for logging.</param>
    public QueryExecutor(
        IRestApiClient apiClient,
        string account,
        ILogger<QueryExecutor> logger)
    {
        ArgumentNullException.ThrowIfNull(apiClient);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentException.ThrowIfNullOrEmpty(account);

        _apiClient = apiClient;
        _logger = logger;

        _accountUrl = account.Contains(".")
            ? $"https://{account}"
            : $"https://{account}.snowflakecomputing.com";
    }

    /// <inheritdoc/>
    public async Task<QueryResult> ExecuteQueryAsync(
        QueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.Statement, nameof(request.Statement));
        ArgumentNullException.ThrowIfNull(request.AuthToken);

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Build the query request
            var queryRequest = RequestBuilder.BuildQueryRequest(
                request.Statement,
                request.Database,
                request.Schema,
                request.Warehouse,
                request.Role,
                (int)request.Timeout.TotalSeconds,
                request.Parameters,
                request.IsMultiStatement);

            // Add query parameters to URL (required by Snowflake v1 API)
            var requestId = Guid.NewGuid().ToString();
            var requestGuid = Guid.NewGuid().ToString();
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();

            // Build endpoint with session ID if available
            var endpoint = $"{_accountUrl}{QueryEndpoint}?requestId={requestId}&request_guid={requestGuid}&startTime={startTime}";

            // Add session ID to maintain session state (required for ALTER SESSION settings to persist)
            if (!string.IsNullOrEmpty(request.AuthToken.SessionId))
            {
                endpoint += $"&sid={request.AuthToken.SessionId}";
            }

            // Execute the query
            var response = await _apiClient.PostAsync<SnowflakeQueryResponse>(
                endpoint,
                queryRequest,
                request.AuthToken,
                cancellationToken);

            stopwatch.Stop();

            if (!response.Success || response.Data == null)
            {
                return new QueryResult
                {
                    Status = QueryStatus.Failed,
                    ExecutionTime = stopwatch.Elapsed,
                    Errors = new List<QueryError>
                    {
                        new QueryError
                        {
                            ErrorCode = response.Code ?? "UNKNOWN",
                            Message = response.Message ?? "Query execution failed."
                        }
                    }
                };
            }

            var data = response.Data;

            // Debug: Check what format we received
            _logger.LogDebug("QueryResultFormat = {QueryResultFormat}", data.QueryResultFormat);
            _logger.LogDebug("Has RowSetBase64 = {HasRowSetBase64}", !string.IsNullOrEmpty(data.RowSetBase64));
            _logger.LogDebug("Chunk Count = {ChunkCount}", data.Chunks?.Count ?? 0);
            _logger.LogDebug("Has RowSet = {HasRowSet}", data.RowSet != null);
            _logger.LogDebug("Has RowType = {HasRowType}", data.RowType != null);

            // Debug: Check if DOTNET_QUERY_RESULT_FORMAT parameter is in the response
            if (data.Parameters != null)
            {
                var arrowParam = data.Parameters.FirstOrDefault(p => p.Name?.Contains("QUERY_RESULT_FORMAT") == true);
                if (arrowParam != null)
                {
                    _logger.LogDebug("Found parameter {ParamName} = {ParamValue}", arrowParam.Name, arrowParam.Value);
                }
                else
                {
                    _logger.LogDebug("DOTNET_QUERY_RESULT_FORMAT parameter not found in response");
                    _logger.LogDebug("Available parameters: {Params}...", string.Join(", ", data.Parameters.Take(5).Select(p => p.Name)));
                }
            }

            // Check if we have Arrow format data in inline payload or remote chunks.
            if (!string.IsNullOrEmpty(data.RowSetBase64) || (data.Chunks?.Count > 0))
            {
                var arrayStream = await ChunkedArrowArrayStream.CreateAsync(
                    _apiClient,
                    request.AuthToken,
                    data.RowSetBase64,
                    data.Chunks,
                    data.ChunkHeaders,
                    data.Qrmk,
                    cancellationToken).ConfigureAwait(false);

                return new QueryResult
                {
                    StatementHandle = data.QueryId ?? string.Empty,
                    Status = QueryStatus.Success,
                    Schema = arrayStream.Schema,
                    ResultStream = arrayStream,
                    RowCount = data.Returned ?? 0,
                    ExecutionTime = stopwatch.Elapsed
                };
            }

            // JSON (rowset/rowtype) format is not supported by this executor.
            // Only Arrow-serialized results (rowsetBase64) are handled.
            if (data.RowType != null && data.RowSet != null)
            {
                return new QueryResult
                {
                    StatementHandle = data.QueryId ?? string.Empty,
                    Status = QueryStatus.Failed,
                    ExecutionTime = stopwatch.Elapsed,
                    Errors =
                    [
                        new QueryError
                        {
                            ErrorCode = "UNSUPPORTED_FORMAT",
                            Message =
                                "JSON rowset/rowtype format is not supported. Request results in Arrow format (set DOTNET_QUERY_RESULT_FORMAT=ARROW)."
                        }
                    ]
                };
            }

            // No results (e.g., DDL/DML statement)
            return new QueryResult
            {
                StatementHandle = data.QueryId ?? string.Empty,
                Status = QueryStatus.Success,
                RowCount = data.Returned ?? 0,
                ExecutionTime = stopwatch.Elapsed
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            stopwatch.Stop();

            return new QueryResult
            {
                Status = QueryStatus.Cancelled,
                ExecutionTime = stopwatch.Elapsed
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            return new QueryResult
            {
                Status = QueryStatus.Failed,
                ExecutionTime = stopwatch.Elapsed,
                Errors = new List<QueryError>
                {
                    new QueryError
                    {
                        ErrorCode = "EXECUTION_ERROR",
                        Message = $"Query execution failed: {ex.Message}"
                    }
                }
            };
        }
    }

    /// <inheritdoc/>
    public async Task<QueryResult> ExecutePreparedStatementAsync(
        PreparedStatement statement,
        ParameterSet parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(statement);
        ArgumentNullException.ThrowIfNull(parameters);

        var request = new QueryRequest
        {
            Statement = statement.Statement,
            Parameters = parameters.Parameters
        };

        return await ExecuteQueryAsync(request, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task CancelQueryAsync(string queryId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(queryId);

        throw new NotImplementedException("Query cancellation not yet implemented");
    }


    // Streams Arrow batches from an ArrowStreamReader without buffering all batches in memory.
    private class ChunkedArrowArrayStream : Ipc.IArrowArrayStream
    {
        private readonly IRestApiClient _apiClient;
        private readonly Services.Authentication.AuthenticationToken _authToken;
        private readonly Dictionary<string, string>? _chunkHeaders;
        private readonly string? _qrmk;
        private readonly Queue<string> _chunkUrls;
        private readonly SemaphoreSlim _gate = new(1, 1);
        private System.IO.Stream _currentStream;
        private Ipc.ArrowStreamReader _currentReader;
        private bool _disposed;

        private ChunkedArrowArrayStream(
            IRestApiClient apiClient,
            Services.Authentication.AuthenticationToken authToken,
            Dictionary<string, string>? chunkHeaders,
            string? qrmk,
            Queue<string> chunkUrls,
            System.IO.Stream currentStream,
            Ipc.ArrowStreamReader currentReader)
        {
            ArgumentNullException.ThrowIfNull(apiClient);
            ArgumentNullException.ThrowIfNull(authToken);
            ArgumentNullException.ThrowIfNull(chunkUrls);
            ArgumentNullException.ThrowIfNull(currentStream);
            ArgumentNullException.ThrowIfNull(currentReader);

            _apiClient = apiClient;
            _authToken = authToken;
            _chunkHeaders = chunkHeaders;
            _qrmk = qrmk;
            _chunkUrls = chunkUrls;
            _currentStream = currentStream;
            _currentReader = currentReader;
        }

        public static async Task<ChunkedArrowArrayStream> CreateAsync(
            IRestApiClient apiClient,
            Services.Authentication.AuthenticationToken authToken,
            string? rowSetBase64,
            List<ChunkInfo>? chunks,
            Dictionary<string, string>? chunkHeaders,
            string? qrmk,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(apiClient);
            ArgumentNullException.ThrowIfNull(authToken);

            var chunkUrls = new Queue<string>((chunks ?? []).Select(c => c.Url).Where(u => !string.IsNullOrWhiteSpace(u)));

            System.IO.Stream stream;
            Ipc.ArrowStreamReader reader;

            if (!string.IsNullOrEmpty(rowSetBase64))
            {
                var arrowBytes = Convert.FromBase64String(rowSetBase64);
                stream = new System.IO.MemoryStream(arrowBytes);
                reader = new Ipc.ArrowStreamReader(stream);
            }
            else if (chunkUrls.Count > 0)
            {
                var url = chunkUrls.Dequeue();
                stream = await apiClient.GetArrowStreamAsync(
                    url,
                    authToken,
                    chunkHeaders,
                    qrmk,
                    cancellationToken).ConfigureAwait(false);
                reader = new Ipc.ArrowStreamReader(stream);
            }
            else
            {
                throw new InvalidOperationException("Arrow result format was requested, but neither rowsetBase64 nor chunks were present.");
            }

            return new ChunkedArrowArrayStream(
                apiClient,
                authToken,
                chunkHeaders,
                qrmk,
                chunkUrls,
                stream,
                reader);
        }

        public Schema Schema => _currentReader.Schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var batch = await _currentReader.ReadNextRecordBatchAsync(cancellationToken);
                    if (batch != null)
                    {
                        return batch;
                    }

                    if (_chunkUrls.Count == 0)
                    {
                        return null;
                    }

                    DisposeCurrentReaderAndStream();

                    var nextUrl = _chunkUrls.Dequeue();
                    _currentStream = await _apiClient.GetArrowStreamAsync(
                        nextUrl,
                        _authToken,
                        _chunkHeaders,
                        _qrmk,
                        cancellationToken).ConfigureAwait(false);
                    _currentReader = new Ipc.ArrowStreamReader(_currentStream);
                }
            }
            finally
            {
                _gate.Release();
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            DisposeCurrentReaderAndStream();
            _gate.Dispose();
        }

        private void DisposeCurrentReaderAndStream()
        {
            try
            {
                _currentReader?.Dispose();
            }
            finally
            {
                _currentStream?.Dispose();
            }
        }
    }

    private class SnowflakeQueryResponse
    {
        [System.Text.Json.Serialization.JsonPropertyName("queryId")]
        public string? QueryId { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("sqlState")]
        public string? SqlState { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("rowtype")]
        public List<RowType>? RowType { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("rowset")]
        public List<List<string>>? RowSet { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("rowsetBase64")]
        public string? RowSetBase64 { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("queryResultFormat")]
        public string? QueryResultFormat { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("total")]
        public long? Total { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("returned")]
        public long? Returned { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("parameters")]
        public List<NameValueParameter>? Parameters { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("chunks")]
        public List<ChunkInfo>? Chunks { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("chunkHeaders")]
        public Dictionary<string, string>? ChunkHeaders { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("qrmk")]
        public string? Qrmk { get; set; }
    }

    private class ChunkInfo
    {
        [System.Text.Json.Serialization.JsonPropertyName("url")]
        public string Url { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("rowCount")]
        public int RowCount { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("uncompressedSize")]
        public int UncompressedSize { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("compressedSize")]
        public int CompressedSize { get; set; }
    }

    private class RowType
    {
        [System.Text.Json.Serialization.JsonPropertyName("name")]
        public string? Name { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("type")]
        public string? Type { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("length")]
        public int? Length { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("precision")]
        public int? Precision { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("scale")]
        public int? Scale { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("nullable")]
        public bool? Nullable { get; set; }
    }

    private class NameValueParameter
    {
        [System.Text.Json.Serialization.JsonPropertyName("name")]
        public string? Name { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("value")]
        public object? Value { get; set; }
    }

}
