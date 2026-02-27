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
using Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;

/// <summary>
/// Provides HTTP communication with Snowflake's REST API.
/// </summary>
public interface IRestApiClient
{
    /// <summary>
    /// Sends a POST request to the specified endpoint.
    /// </summary>
    /// <typeparam name="T">The response type.</typeparam>
    /// <param name="endpoint">The API endpoint.</param>
    /// <param name="request">The request payload.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API response.</returns>
    Task<ApiResponse<T>> PostAsync<T>(
        string endpoint,
        object request,
        AuthenticationToken token,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets an Arrow stream from the specified URL.
    /// </summary>
    /// <param name="url">The URL to fetch the Arrow stream from.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A stream containing Arrow data.</returns>
    Task<Stream> GetArrowStreamAsync(
        string url,
        AuthenticationToken token,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a GET request to the specified endpoint.
    /// </summary>
    /// <typeparam name="T">The response type.</typeparam>
    /// <param name="endpoint">The API endpoint.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API response.</returns>
    Task<ApiResponse<T>> GetAsync<T>(
        string endpoint,
        AuthenticationToken token,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents an API response from Snowflake.
/// </summary>
/// <typeparam name="T">The response data type.</typeparam>
public class ApiResponse<T>
{
    /// <summary>
    /// Gets or sets a value indicating whether the request was successful.
    /// </summary>
    [System.Text.Json.Serialization.JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>
    /// Gets or sets the response message.
    /// </summary>
    [System.Text.Json.Serialization.JsonPropertyName("message")]
    public string? Message { get; set; }

    /// <summary>
    /// Gets or sets the response data.
    /// </summary>
    [System.Text.Json.Serialization.JsonPropertyName("data")]
    public T? Data { get; set; }

    /// <summary>
    /// Gets or sets the error code (if any).
    /// </summary>
    [System.Text.Json.Serialization.JsonPropertyName("code")]
    public string? Code { get; set; }
}
