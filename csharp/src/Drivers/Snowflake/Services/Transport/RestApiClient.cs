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
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Transport;

/// <summary>
/// Implements HTTP communication with Snowflake's REST API.
/// </summary>
public class RestApiClient : IRestApiClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _enableCompression;
    private readonly int _maxRetries;
    private readonly TimeSpan _baseRetryDelay;

    /// <summary>
    /// Initializes a new instance of the <see cref="RestApiClient"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="enableCompression">Whether to enable compression.</param>
    /// <param name="maxRetries">Maximum number of retries for transient errors.</param>
    /// <param name="baseRetryDelay">Base delay for exponential backoff.</param>
    public RestApiClient(
        HttpClient httpClient,
        bool enableCompression = true,
        int maxRetries = 3,
        TimeSpan? baseRetryDelay = null)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _enableCompression = enableCompression;
        _maxRetries = maxRetries;
        _baseRetryDelay = baseRetryDelay ?? TimeSpan.FromMilliseconds(100);
    }

    /// <inheritdoc/>
    public async Task<ApiResponse<T>> PostAsync<T>(
        string endpoint,
        object request,
        AuthenticationToken token,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(endpoint))
            throw new ArgumentException("Endpoint cannot be null or empty.", nameof(endpoint));
        
        if (token == null)
            throw new ArgumentNullException(nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Post, endpoint);
            ConfigureRequest(requestMessage, token);
            
            requestMessage.Content = JsonContent.Create(request);
            
            // Debug: Log the request
            var requestJson = await requestMessage.Content.ReadAsStringAsync();
            Console.WriteLine($"DEBUG REQUEST to {endpoint}:");
            Console.WriteLine(requestJson);
            
            if (_enableCompression)
            {
                requestMessage.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
                requestMessage.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            }

            var response = await _httpClient.SendAsync(requestMessage, cancellationToken);
            
            // Debug: Log response status
            Console.WriteLine($"DEBUG RESPONSE: {(int)response.StatusCode} {response.StatusCode}");
            
            response.EnsureSuccessStatusCode();

            // Handle compressed responses
            var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            if (response.Content.Headers.ContentEncoding.Contains("gzip"))
            {
                stream = new GZipStream(stream, CompressionMode.Decompress);
            }
            else if (response.Content.Headers.ContentEncoding.Contains("deflate"))
            {
                stream = new DeflateStream(stream, CompressionMode.Decompress);
            }

            // Debug: Read and log the JSON
            using var reader = new StreamReader(stream);
            var json = await reader.ReadToEndAsync(cancellationToken);
            Console.WriteLine($"DEBUG JSON RESPONSE: {json.Substring(0, Math.Min(500, json.Length))}...");

            var result = JsonSerializer.Deserialize<ApiResponse<T>>(json)
                ?? throw new InvalidOperationException("Failed to deserialize API response.");
            
            return result;
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<Stream> GetArrowStreamAsync(
        string url,
        AuthenticationToken token,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(url))
            throw new ArgumentException("URL cannot be null or empty.", nameof(url));
        
        if (token == null)
            throw new ArgumentNullException(nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);
            ConfigureRequest(requestMessage, token);
            
            requestMessage.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.apache.arrow.stream"));

            var response = await _httpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            
            // Handle compressed responses
            if (response.Content.Headers.ContentEncoding.Contains("gzip"))
            {
                return new GZipStream(stream, CompressionMode.Decompress);
            }
            else if (response.Content.Headers.ContentEncoding.Contains("deflate"))
            {
                return new DeflateStream(stream, CompressionMode.Decompress);
            }

            return stream;
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<ApiResponse<T>> GetAsync<T>(
        string endpoint,
        AuthenticationToken token,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(endpoint))
            throw new ArgumentException("Endpoint cannot be null or empty.", nameof(endpoint));
        
        if (token == null)
            throw new ArgumentNullException(nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Get, endpoint);
            ConfigureRequest(requestMessage, token);

            var response = await _httpClient.SendAsync(requestMessage, cancellationToken);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadFromJsonAsync<ApiResponse<T>>(cancellationToken)
                ?? throw new InvalidOperationException("Failed to deserialize API response.");
        }, cancellationToken);
    }

    private void ConfigureRequest(HttpRequestMessage request, AuthenticationToken token)
    {
        // Add authentication header - Snowflake uses "Snowflake Token=" format with session token
        // Reference: snowflake-connector-net uses: "Snowflake Token=\"{sessionToken}\""
        var sessionToken = token.SessionToken ?? token.AccessToken;
        var authHeader = $"Snowflake Token=\"{sessionToken}\"";
        Console.WriteLine($"DEBUG AUTH: Using session token (first 20 chars): {sessionToken.Substring(0, Math.Min(20, sessionToken.Length))}...");
        Console.WriteLine($"DEBUG AUTH: Has SessionToken: {token.SessionToken != null}, Has MasterToken: {token.MasterToken != null}");
        request.Headers.Add("Authorization", authHeader);
        
        // Add Accept header - v1 API uses application/snowflake, not Arrow
        // Reference: snowflake-connector-net uses "application/snowflake"
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/snowflake"));
        
        // Add user agent - match Snowflake connector format to enable Arrow support
        // Reference: snowflake-connector-net sends ".NET/{version}" as driver name
        request.Headers.UserAgent.ParseAdd(".NET/1.0.0");
        request.Headers.UserAgent.ParseAdd("(Windows)");
        request.Headers.UserAgent.ParseAdd(".NETCoreApp/8.0");
    }

    private async Task<TResult> ExecuteWithRetryAsync<TResult>(
        Func<Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        Exception? lastException = null;

        while (attempt < _maxRetries)
        {
            try
            {
                return await operation();
            }
            catch (HttpRequestException ex) when (IsTransientError(ex) && attempt < _maxRetries - 1)
            {
                lastException = ex;
                attempt++;
                
                // Exponential backoff with jitter
                var delay = TimeSpan.FromMilliseconds(
                    _baseRetryDelay.TotalMilliseconds * Math.Pow(2, attempt) +
                    Random.Shared.Next(0, 100));
                
                await Task.Delay(delay, cancellationToken);
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException && attempt < _maxRetries - 1)
            {
                lastException = ex;
                attempt++;
                
                var delay = TimeSpan.FromMilliseconds(
                    _baseRetryDelay.TotalMilliseconds * Math.Pow(2, attempt) +
                    Random.Shared.Next(0, 100));
                
                await Task.Delay(delay, cancellationToken);
            }
        }

        throw lastException ?? new InvalidOperationException("Operation failed after retries.");
    }

    private static bool IsTransientError(HttpRequestException ex)
    {
        // Check for transient HTTP status codes
        if (ex.StatusCode.HasValue)
        {
            var statusCode = (int)ex.StatusCode.Value;
            return statusCode == 408 || // Request Timeout
                   statusCode == 429 || // Too Many Requests
                   statusCode == 503 || // Service Unavailable
                   statusCode == 504;   // Gateway Timeout
        }

        // Check for network-related errors
        return ex.InnerException is System.Net.Sockets.SocketException ||
               ex.InnerException is IOException;
    }
}
