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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Query;

/// <summary>
/// Implements query execution for Snowflake connections.
/// </summary>
internal class QueryExecutor : IQueryExecutor
{
    private readonly IRestApiClient _apiClient;
    private readonly string _accountUrl;
    private readonly ILogger<QueryExecutor> _logger;
    private const string QueryEndpoint = "/queries/v1/query-request";

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
    /// </summary>
    /// <param name="apiClient">The REST API client.</param>
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
        var authToken = request.AuthToken;

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var queryRequest = BuildQueryRequest(request, out string endpoint);
            var response = await _apiClient.PostAsync<SnowflakeQueryResponse>(
                endpoint,
                queryRequest,
                authToken,
                cancellationToken);

            stopwatch.Stop();

            if (!response.Success || response.Data == null)
                return CreateFailedResponseResult(response, stopwatch.Elapsed);

            var data = response.Data;
            _logger.LogDebug(
                "QueryResultFormat={QueryResultFormat}, HasRowSetBase64={HasRowSetBase64}, ChunkCount={ChunkCount}, HasRowSet={HasRowSet}, HasRowType={HasRowType}",
                data.QueryResultFormat,
                !string.IsNullOrEmpty(data.RowSetBase64),
                data.Chunks?.Count ?? 0,
                data.RowSet != null,
                data.RowType != null);

            if (HasArrowResult(data))
                return await CreateSuccessResultAsync(data, authToken, cancellationToken, stopwatch.Elapsed).ConfigureAwait(false);

            return IsUnsupportedJsonResult(data) ? CreateUnsupportedFormatResult(data, stopwatch.Elapsed) : CreateNoResultSuccess(data, stopwatch.Elapsed);
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

    private static bool HasArrowResult(SnowflakeQueryResponse data) =>
        !string.IsNullOrEmpty(data.RowSetBase64) || (data.Chunks?.Count > 0);

    private static bool IsUnsupportedJsonResult(SnowflakeQueryResponse data) =>
        data is { RowType: not null, RowSet: not null };

    private static QueryResult CreateFailedResponseResult(ApiResponse<SnowflakeQueryResponse> response, TimeSpan executionTime) =>
        new()
        {
            Status = QueryStatus.Failed,
            ExecutionTime = executionTime,
            Errors =
            [
                new QueryError
                {
                    ErrorCode = response.Code ?? "UNKNOWN",
                    Message = response.Message ?? "Query execution failed."
                }
            ]
        };

    private static QueryResult CreateUnsupportedFormatResult(SnowflakeQueryResponse data, TimeSpan executionTime) =>
        new()
        {
            StatementHandle = data.QueryId ?? string.Empty,
            Status = QueryStatus.Failed,
            ExecutionTime = executionTime,
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

    private static QueryResult CreateNoResultSuccess(SnowflakeQueryResponse data, TimeSpan executionTime) =>
        new()
        {
            StatementHandle = data.QueryId ?? string.Empty,
            Status = QueryStatus.Success,
            RowCount = data.Returned ?? 0,
            ExecutionTime = executionTime
        };

    private async Task<QueryResult> CreateSuccessResultAsync(
        SnowflakeQueryResponse data,
        Authentication.AuthenticationToken authToken,
        CancellationToken cancellationToken,
        TimeSpan executionTime)
    {
        var arrayStream = await ChunkedArrowArrayStream.CreateAsync(
            _apiClient,
            authToken,
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
            ExecutionTime = executionTime
        };
    }

    private object BuildQueryRequest(QueryRequest request, out string endpoint)
    {
        var queryRequest = RequestBuilder.BuildQueryRequest(
            request.Statement,
            request.Database,
            request.Schema,
            request.Warehouse,
            request.Role,
            (int)request.Timeout.TotalSeconds,
            request.Parameters,
            request.IsMultiStatement);

        var requestId = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid().ToString();
        var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
        endpoint = $"{_accountUrl}{QueryEndpoint}?requestId={requestId}&request_guid={requestGuid}&startTime={startTime}";
        var sessionId = request.AuthToken?.SessionId;
        if (!string.IsNullOrEmpty(sessionId))
            endpoint += $"&sid={sessionId}";
        return queryRequest;
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

}
