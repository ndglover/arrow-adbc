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
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.TypeConversion;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services;

/// <summary>
/// Implements query execution for Snowflake connections.
/// </summary>
public class QueryExecutor : IQueryExecutor
{
    private readonly IRestApiClient _apiClient;
    private readonly ITypeConverter _typeConverter;
    private readonly string _accountUrl;
    private const string QueryEndpoint = "/queries/v1/query-request";
    private const string CancelEndpoint = "/queries/{0}/cancel";

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
    /// </summary>
    /// <param name="apiClient">The REST API client.</param>
    /// <param name="typeConverter">The type converter.</param>
    /// <param name="account">The Snowflake account identifier.</param>
    public QueryExecutor(
        IRestApiClient apiClient,
        ITypeConverter typeConverter,
        string account)
    {
        _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
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

            // Debug: Check what format we received
            Console.WriteLine($"DEBUG: QueryResultFormat = {data.QueryResultFormat}");
            Console.WriteLine($"DEBUG: Has RowSetBase64 = {!string.IsNullOrEmpty(data.RowSetBase64)}");
            Console.WriteLine($"DEBUG: Has RowSet = {data.RowSet != null}");
            Console.WriteLine($"DEBUG: Has RowType = {data.RowType != null}");
            
            // Debug: Check if DOTNET_QUERY_RESULT_FORMAT parameter is in the response
            if (data.Parameters != null)
            {
                var arrowParam = data.Parameters.FirstOrDefault(p => p.Name?.Contains("QUERY_RESULT_FORMAT") == true);
                if (arrowParam != null)
                {
                    Console.WriteLine($"DEBUG: Found parameter {arrowParam.Name} = {arrowParam.Value}");
                }
                else
                {
                    Console.WriteLine($"DEBUG: DOTNET_QUERY_RESULT_FORMAT parameter not found in response");
                    Console.WriteLine($"DEBUG: Available parameters: {string.Join(", ", data.Parameters.Take(5).Select(p => p.Name))}...");
                }
            }

            // Check if we have Arrow format data
            if (!string.IsNullOrEmpty(data.RowSetBase64))
            {
                // Decode base64 Arrow data
                var arrowBytes = Convert.FromBase64String(data.RowSetBase64);
                using var stream = new System.IO.MemoryStream(arrowBytes);
                using var arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
                
                // Read the schema and all record batches
                var schema = arrowReader.Schema;
                var recordBatches = new List<Apache.Arrow.RecordBatch>();
                
                while (true)
                {
                    var batch = arrowReader.ReadNextRecordBatch();
                    if (batch == null)
                        break;
                    recordBatches.Add(batch);
                }
                
                var arrayStream = new SimpleArrowArrayStream(schema, recordBatches);
                
                return new QueryResult
                {
                    StatementHandle = data.QueryId ?? string.Empty,
                    Status = QueryStatus.Success,
                    Schema = schema,
                    ResultStream = arrayStream,
                    RowCount = data.Returned ?? 0,
                    ExecutionTime = stopwatch.Elapsed
                };
            }
            
            // Check if we have JSON format data (rowset and rowtype)
            if (data.RowType != null && data.RowSet != null)
            {
                // Convert Snowflake rowset to Arrow format
                var schema = ConvertRowTypeToArrowSchema(data.RowType);
                var arrayStream = ConvertRowSetToArrowStream(schema, data.RowSet);

                return new QueryResult
                {
                    StatementHandle = data.QueryId ?? string.Empty,
                    Status = QueryStatus.Success,
                    Schema = schema,
                    ResultStream = arrayStream,
                    RowCount = data.Returned ?? 0,
                    ExecutionTime = stopwatch.Elapsed
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

        throw new NotImplementedException("Query cancellation not yet implemented");
    }

    private static QueryStatus ParseQueryStatus(string? statusUrl)
    {
        return QueryStatus.Success;
    }

    private Apache.Arrow.Schema ConvertRowTypeToArrowSchema(List<RowType> rowTypes)
    {
        var fields = new List<Apache.Arrow.Field>();
        
        foreach (var rowType in rowTypes)
        {
            var snowflakeType = new SnowflakeDataType
            {
                TypeName = rowType.Type ?? "TEXT",
                Precision = rowType.Precision,
                Scale = rowType.Scale,
                Length = rowType.Length,
                IsNullable = rowType.Nullable ?? true
            };
            
            var arrowType = _typeConverter.ConvertSnowflakeTypeToArrow(snowflakeType);
            
            fields.Add(new Apache.Arrow.Field(
                rowType.Name ?? "column",
                arrowType,
                rowType.Nullable ?? true));
        }
        
        return new Apache.Arrow.Schema(fields, null);
    }

    private Apache.Arrow.Ipc.IArrowArrayStream ConvertRowSetToArrowStream(
        Apache.Arrow.Schema schema,
        List<List<string>> rowSet)
    {
        // Create Arrow arrays from the rowset
        var recordBatches = new List<Apache.Arrow.RecordBatch>();
        
        if (rowSet.Count > 0)
        {
            var builders = new List<Apache.Arrow.StringArray.Builder>();
            
            // Create builders for each column
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                builders.Add(new Apache.Arrow.StringArray.Builder());
            }
            
            // Add rows
            foreach (var row in rowSet)
            {
                for (int i = 0; i < row.Count && i < builders.Count; i++)
                {
                    if (row[i] == null)
                    {
                        builders[i].AppendNull();
                    }
                    else
                    {
                        builders[i].Append(row[i]);
                    }
                }
            }
            
            // Build arrays
            var arrays = builders.Select(b => b.Build()).ToArray();
            var recordBatch = new Apache.Arrow.RecordBatch(schema, arrays, rowSet.Count);
            recordBatches.Add(recordBatch);
        }
        
        // Create a simple array stream implementation
        return new SimpleArrowArrayStream(schema, recordBatches);
    }

    // Simple implementation of IArrowArrayStream for in-memory record batches
    private class SimpleArrowArrayStream : Apache.Arrow.Ipc.IArrowArrayStream
    {
        private readonly Apache.Arrow.Schema _schema;
        private readonly List<Apache.Arrow.RecordBatch> _batches;
        private int _currentIndex = 0;

        public SimpleArrowArrayStream(Apache.Arrow.Schema schema, List<Apache.Arrow.RecordBatch> batches)
        {
            _schema = schema;
            _batches = batches;
        }

        public Apache.Arrow.Schema Schema => _schema;

        public ValueTask<Apache.Arrow.RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_currentIndex < _batches.Count)
            {
                return new ValueTask<Apache.Arrow.RecordBatch?>(_batches[_currentIndex++]);
            }
            return new ValueTask<Apache.Arrow.RecordBatch?>((Apache.Arrow.RecordBatch?)null);
        }

        public void Dispose()
        {
            foreach (var batch in _batches)
            {
                batch?.Dispose();
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

    private class ResultSetMetaData
    {
        [System.Text.Json.Serialization.JsonPropertyName("rowType")]
        public string? RowType { get; set; }
        
        [System.Text.Json.Serialization.JsonPropertyName("url")]
        public string? Url { get; set; }
        
        [System.Text.Json.Serialization.JsonPropertyName("format")]
        public string? Format { get; set; }
    }
}
