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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.TypeConversion;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services;

/// <summary>
/// Manages prepared statements for Snowflake connections.
/// </summary>
public class PreparedStatementManager
{
    private readonly IRestApiClient _apiClient;
    private readonly ITypeConverter _typeConverter;
    private readonly string _accountUrl;
    private readonly ConcurrentDictionary<string, PreparedStatement> _statementCache;
    private const string PrepareEndpoint = "/api/v2/statements";

    /// <summary>
    /// Initializes a new instance of the <see cref="PreparedStatementManager"/> class.
    /// </summary>
    /// <param name="apiClient">The REST API client.</param>
    /// <param name="typeConverter">The type converter.</param>
    /// <param name="account">The Snowflake account identifier.</param>
    public PreparedStatementManager(
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

        _statementCache = new ConcurrentDictionary<string, PreparedStatement>();
    }

    /// <summary>
    /// Prepares a SQL statement for execution.
    /// </summary>
    /// <param name="statement">The SQL statement to prepare.</param>
    /// <param name="database">The database context (optional).</param>
    /// <param name="schema">The schema context (optional).</param>
    /// <param name="warehouse">The warehouse to use (optional).</param>
    /// <param name="role">The role to use (optional).</param>
    /// <param name="authToken">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A prepared statement.</returns>
    public async Task<PreparedStatement> PrepareAsync(
        string statement,
        string? database = null,
        string? schema = null,
        string? warehouse = null,
        string? role = null,
        AuthenticationToken? authToken = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(statement))
            throw new ArgumentException("Statement cannot be null or empty.", nameof(statement));

        if (authToken == null)
            throw new ArgumentNullException(nameof(authToken));

        // Check cache first
        var cacheKey = GenerateCacheKey(statement, database, schema);
        if (_statementCache.TryGetValue(cacheKey, out var cachedStatement))
            return cachedStatement;

        try
        {
            // Build prepare request
            var prepareRequest = RequestBuilder.BuildPrepareRequest(
                statement,
                database,
                schema,
                warehouse,
                role);

            // Send prepare request
            var endpoint = $"{_accountUrl}{PrepareEndpoint}";
            var response = await _apiClient.PostAsync<PrepareResponseData>(
                endpoint,
                prepareRequest,
                authToken,
                cancellationToken);

            if (!response.Success || response.Data == null)
            {
                throw new InvalidOperationException(
                    $"Failed to prepare statement: {response.Message ?? "Unknown error"}");
            }

            var preparedStatement = new PreparedStatement
            {
                StatementHandle = response.Data.StatementHandle ?? string.Empty,
                Statement = statement,
                ParameterSchema = response.Data.ParameterMetaData != null
                    ? BuildParameterSchema(response.Data.ParameterMetaData)
                    : null,
                ResultSchema = response.Data.ResultSetMetaData != null
                    ? BuildResultSchema(response.Data.ResultSetMetaData)
                    : null
            };

            // Cache the prepared statement
            _statementCache.TryAdd(cacheKey, preparedStatement);

            return preparedStatement;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to prepare statement: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Executes a prepared statement with a single parameter set.
    /// </summary>
    /// <param name="statement">The prepared statement.</param>
    /// <param name="parameters">The parameter values.</param>
    /// <param name="authToken">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The query result.</returns>
    public async Task<QueryResult> ExecuteAsync(
        PreparedStatement statement,
        ParameterSet parameters,
        AuthenticationToken authToken,
        CancellationToken cancellationToken = default)
    {
        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));

        if (authToken == null)
            throw new ArgumentNullException(nameof(authToken));

        // Validate parameters against schema
        if (statement.ParameterSchema != null)
            ValidateParameters(statement.ParameterSchema, parameters);

        // Build execution request
        var executeRequest = new
        {
            statementHandle = statement.StatementHandle,
            bindings = parameters.Parameters
        };

        var endpoint = $"{_accountUrl}{PrepareEndpoint}";
        var response = await _apiClient.PostAsync<QueryResponseData>(
            endpoint,
            executeRequest,
            authToken,
            cancellationToken);

        if (!response.Success || response.Data == null)
        {
            return new QueryResult
            {
                Status = QueryStatus.Failed,
                Errors = new List<QueryError>
                {
                    new QueryError
                    {
                        ErrorCode = response.Code ?? "UNKNOWN",
                        Message = response.Message ?? "Execution failed."
                    }
                }
            };
        }

        return new QueryResult
        {
            StatementHandle = response.Data.StatementHandle ?? string.Empty,
            Status = QueryStatus.Success,
            RowCount = response.Data.RowCount ?? 0
        };
    }

    /// <summary>
    /// Executes a prepared statement with multiple parameter sets (batch execution).
    /// </summary>
    /// <param name="statement">The prepared statement.</param>
    /// <param name="parameterSets">The list of parameter sets.</param>
    /// <param name="authToken">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of query results.</returns>
    public async Task<List<QueryResult>> ExecuteBatchAsync(
        PreparedStatement statement,
        List<ParameterSet> parameterSets,
        AuthenticationToken authToken,
        CancellationToken cancellationToken = default)
    {
        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        if (parameterSets == null || parameterSets.Count == 0)
            throw new ArgumentException("Parameter sets cannot be null or empty.", nameof(parameterSets));

        if (authToken == null)
            throw new ArgumentNullException(nameof(authToken));

        var results = new List<QueryResult>();

        // Validate all parameter sets
        if (statement.ParameterSchema != null)
        {
            foreach (var paramSet in parameterSets)
            {
                ValidateParameters(statement.ParameterSchema, paramSet);
            }
        }

        // Build batch execution request
        var batchRequest = RequestBuilder.BuildBatchExecuteRequest(
            statement.StatementHandle,
            parameterSets.Select(ps => ps.Parameters).ToList());

        var endpoint = $"{_accountUrl}{PrepareEndpoint}";
        var response = await _apiClient.PostAsync<BatchResponseData>(
            endpoint,
            batchRequest,
            authToken,
            cancellationToken);

        if (!response.Success || response.Data == null)
        {
            results.Add(new QueryResult
            {
                Status = QueryStatus.Failed,
                Errors = new List<QueryError>
                {
                    new QueryError
                    {
                        ErrorCode = response.Code ?? "UNKNOWN",
                        Message = response.Message ?? "Batch execution failed."
                    }
                }
            });
            return results;
        }

        // Process batch results
        foreach (var resultData in response.Data.Results ?? new List<BatchResultData>())
        {
            results.Add(new QueryResult
            {
                StatementHandle = resultData.StatementHandle ?? string.Empty,
                Status = QueryStatus.Success,
                RowCount = resultData.RowCount ?? 0
            });
        }

        return results;
    }

    /// <summary>
    /// Closes a prepared statement and releases its resources.
    /// </summary>
    /// <param name="statement">The prepared statement to close.</param>
    public void Close(PreparedStatement statement)
    {
        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        // Remove from cache
        var keysToRemove = _statementCache
            .Where(kvp => kvp.Value.StatementHandle == statement.StatementHandle)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _statementCache.TryRemove(key, out _);
        }
    }

    private static string GenerateCacheKey(string statement, string? database, string? schema)
    {
        return $"{database ?? ""}|{schema ?? ""}|{statement}";
    }

    private Schema BuildParameterSchema(ParameterMetaData metaData)
    {
        // Build Arrow schema from parameter metadata
        var fields = metaData.Parameters.Select(param =>
            new Field(
                param.Name,
                _typeConverter.ConvertSnowflakeTypeToArrow(param.DataType),
                param.DataType.IsNullable)
        ).ToList();

        return new Schema(fields, null);
    }

    private Schema BuildResultSchema(ResultSetMetaData metaData)
    {
        // Build Arrow schema from result set metadata
        var fields = metaData.Columns.Select(col =>
            new Field(
                col.Name,
                _typeConverter.ConvertSnowflakeTypeToArrow(col.DataType),
                col.DataType.IsNullable)
        ).ToList();

        return new Schema(fields, null);
    }

    private static void ValidateParameters(Schema parameterSchema, ParameterSet parameters)
    {
        foreach (var field in parameterSchema.FieldsList)
        {
            if (!parameters.Parameters.ContainsKey(field.Name) && !field.IsNullable)
            {
                throw new ArgumentException(
                    $"Required parameter '{field.Name}' is missing.",
                    nameof(parameters));
            }
        }
    }

    private class PrepareResponseData
    {
        public string? StatementHandle { get; set; }
        public ParameterMetaData? ParameterMetaData { get; set; }
        public ResultSetMetaData? ResultSetMetaData { get; set; }
    }

    private class ParameterMetaData
    {
        public List<ParameterInfo> Parameters { get; set; } = new();
    }

    private class ParameterInfo
    {
        public string Name { get; set; } = string.Empty;
        public SnowflakeDataType DataType { get; set; } = new();
    }

    private class ResultSetMetaData
    {
        public List<ColumnInfo> Columns { get; set; } = new();
    }

    private class ColumnInfo
    {
        public string Name { get; set; } = string.Empty;
        public SnowflakeDataType DataType { get; set; } = new();
    }

    private class QueryResponseData
    {
        public string? StatementHandle { get; set; }
        public long? RowCount { get; set; }
    }

    private class BatchResponseData
    {
        public List<BatchResultData>? Results { get; set; }
    }

    private class BatchResultData
    {
        public string? StatementHandle { get; set; }
        public long? RowCount { get; set; }
    }
}
