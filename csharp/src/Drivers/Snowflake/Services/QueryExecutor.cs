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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.TypeConversion;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services
{
    /// <summary>
    /// Implements query execution for Snowflake connections.
    /// </summary>
    public class QueryExecutor : IQueryExecutor
    {
        private readonly IRestApiClient _apiClient;
        private readonly IArrowStreamReader _streamReader;
        private readonly ITypeConverter _typeConverter;
        private readonly string _accountUrl;
        private const string QueryEndpoint = "/api/v2/statements";
        private const string CancelEndpoint = "/api/v2/statements/{0}/cancel";

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
        /// </summary>
        /// <param name="apiClient">The REST API client.</param>
        /// <param name="streamReader">The Arrow stream reader.</param>
        /// <param name="typeConverter">The type converter.</param>
        /// <param name="account">The Snowflake account identifier.</param>
        public QueryExecutor(
            IRestApiClient apiClient,
            IArrowStreamReader streamReader,
            ITypeConverter typeConverter,
            string account)
        {
            _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
            _streamReader = streamReader ?? throw new ArgumentNullException(nameof(streamReader));
            _typeConverter = typeConverter ?? throw new ArgumentNullException(nameof(typeConverter));
            
            if (string.IsNullOrEmpty(account))
                throw new ArgumentException("Account cannot be null or empty.", nameof(account));

            _accountUrl = account.Contains(".")
                ? $"https://{account}"
                : $"https://{account}.snowflakecomputing.com";
        }

        /// <inheritdoc/>
        public async Task<QueryResult> ExecuteQueryAsync(
            QueryRequest request,
            CancellationToken cancellationToken = default)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));

            if (string.IsNullOrEmpty(request.Statement))
                throw new ArgumentException("Statement cannot be null or empty.", nameof(request));

            if (request.AuthToken == null)
                throw new ArgumentException("Authentication token is required.", nameof(request));

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

                // Execute the query
                var endpoint = $"{_accountUrl}{QueryEndpoint}";
                var response = await _apiClient.PostAsync<QueryResponseData>(
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
                        Errors = new System.Collections.Generic.List<QueryError>
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

                // Check if we need to fetch results from a separate URL
                if (!string.IsNullOrEmpty(data.ResultSetMetaData?.RowType) && 
                    !string.IsNullOrEmpty(data.ResultSetMetaData?.Url))
                {
                    // Fetch Arrow stream from the result URL
                    var arrowStream = await _apiClient.GetArrowStreamAsync(
                        data.ResultSetMetaData.Url,
                        request.AuthToken,
                        cancellationToken);

                    var arrayStream = await _streamReader.ReadStreamAsync(arrowStream, cancellationToken);

                    return new QueryResult
                    {
                        StatementHandle = data.StatementHandle ?? string.Empty,
                        Status = ParseQueryStatus(data.StatementStatusUrl),
                        Schema = arrayStream.Schema,
                        ResultStream = arrayStream,
                        RowCount = data.RowCount ?? 0,
                        ExecutionTime = stopwatch.Elapsed
                    };
                }

                // No results (e.g., DDL/DML statement)
                return new QueryResult
                {
                    StatementHandle = data.StatementHandle ?? string.Empty,
                    Status = QueryStatus.Success,
                    RowCount = data.RowCount ?? 0,
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
                    Errors = new System.Collections.Generic.List<QueryError>
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
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            // For now, execute as a regular query with parameter substitution
            // A full implementation would use Snowflake's prepared statement API
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
            if (string.IsNullOrEmpty(queryId))
                throw new ArgumentException("Query ID cannot be null or empty.", nameof(queryId));

            try
            {
                var endpoint = $"{_accountUrl}{string.Format(CancelEndpoint, queryId)}";
                var cancelRequest = RequestBuilder.BuildCancelRequest(queryId);
                
                // Note: This requires the authentication token to be available
                // In a real implementation, we would need to pass the token through the method signature
                // or maintain it in the executor context
                
                // For now, we'll throw NotImplementedException to indicate this needs proper token management
                throw new NotImplementedException(
                    "Query cancellation requires authentication token management to be implemented.");
            }
            catch (Exception ex) when (ex is not NotImplementedException)
            {
                throw new InvalidOperationException($"Failed to cancel query {queryId}: {ex.Message}", ex);
            }
        }

        private static QueryStatus ParseQueryStatus(string? statusUrl)
        {
            // In a real implementation, we would poll the status URL
            // For now, assume success if we got results
            return QueryStatus.Success;
        }

        private class QueryResponseData
        {
            public string? StatementHandle { get; set; }
            public string? StatementStatusUrl { get; set; }
            public long? RowCount { get; set; }
            public ResultSetMetaData? ResultSetMetaData { get; set; }
        }

        private class ResultSetMetaData
        {
            public string? RowType { get; set; }
            public string? Url { get; set; }
            public string? Format { get; set; }
        }
    }
}
