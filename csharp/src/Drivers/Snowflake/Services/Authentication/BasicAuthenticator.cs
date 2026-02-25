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
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication
{
    /// <summary>
    /// Implements basic username/password authentication for Snowflake.
    /// </summary>
    public class BasicAuthenticator : IBasicAuthenticator
    {
        private readonly HttpClient _httpClient;
        private const string LoginEndpoint = "/session/v1/login-request";

        /// <summary>
        /// Initializes a new instance of the <see cref="BasicAuthenticator"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client for making requests.</param>
        public BasicAuthenticator(HttpClient httpClient)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        }

        /// <inheritdoc/>
        public Task<AuthenticationToken> AuthenticateAsync(
            string account,
            string user,
            string password,
            CancellationToken cancellationToken = default)
        {
            return AuthenticateAsync(account, user, password, null, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<AuthenticationToken> AuthenticateAsync(
            string account,
            string user,
            string password,
            ConnectionConfig? config,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(account))
                throw new ArgumentException("Account cannot be null or empty.", nameof(account));
            
            if (string.IsNullOrEmpty(user))
                throw new ArgumentException("User cannot be null or empty.", nameof(user));
            
            if (string.IsNullOrEmpty(password))
                throw new ArgumentException("Password cannot be null or empty.", nameof(password));

            var loginUrl = BuildLoginUrl(account, config);
            var loginRequest = new LoginRequestBody
            {
                Data = new LoginRequestData
                {
                    // Use .NET as CLIENT_APP_ID to match official Snowflake connector
                    // This is required for Snowflake to enable Arrow format support
                    CLIENT_APP_ID = ".NET",
                    CLIENT_APP_VERSION = "3.1.0", // Match a known version that supports Arrow
                    ACCOUNT_NAME = account,
                    LOGIN_NAME = user,
                    PASSWORD = password,
                    AUTHENTICATOR = "snowflake",
                    CLIENT_ENVIRONMENT = ClientEnvironment.Create(),
                    SESSION_PARAMETERS = new System.Collections.Generic.Dictionary<string, object>
                    {
                        { "DOTNET_QUERY_RESULT_FORMAT", "ARROW" }
                    }
                }
            };

            try
            {
                var response = await _httpClient.PostAsJsonAsync(loginUrl, loginRequest, cancellationToken);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadFromJsonAsync<LoginResponse>(cancellationToken);
                
                if (responseContent?.Data == null)
                    throw new AdbcException("Invalid response from Snowflake authentication service.");

                if (!responseContent.Success)
                {
                    var errorMessage = responseContent.Message ?? "Authentication failed.";
                    throw new AdbcException($"Snowflake authentication failed: {errorMessage}");
                }

                return new AuthenticationToken
                {
                    AccessToken = responseContent.Data.Token ?? throw new AdbcException("No token received from Snowflake."),
                    SessionToken = responseContent.Data.Token, // The token field IS the session token
                    SessionId = responseContent.Data.SessionId?.ToString(),
                    MasterToken = responseContent.Data.MasterToken,
                    ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.Data.MasterTokenValidityInSeconds),
                    TokenType = "Snowflake"
                };
            }
            catch (HttpRequestException ex)
            {
                throw new AdbcException($"Failed to authenticate with Snowflake: {ex.Message}", ex);
            }
            catch (JsonException ex)
            {
                throw new AdbcException($"Failed to parse Snowflake authentication response: {ex.Message}", ex);
            }
        }

        private static string BuildLoginUrl(string account, ConnectionConfig? config)
        {
            // Snowflake account URL format: https://<account>.snowflakecomputing.com
            var accountUrl = account.Contains(".")
                ? $"https://{account}"
                : $"https://{account}.snowflakecomputing.com";
            
            var uriBuilder = new UriBuilder($"{accountUrl}{LoginEndpoint}");
            var query = HttpUtility.ParseQueryString(string.Empty);

            // Add optional parameters if provided
            if (config != null)
            {
                if (!string.IsNullOrEmpty(config.Warehouse))
                    query["warehouse"] = config.Warehouse;
                
                if (!string.IsNullOrEmpty(config.Database))
                    query["databaseName"] = config.Database;
                
                if (!string.IsNullOrEmpty(config.Schema))
                    query["schemaName"] = config.Schema;
                
                if (!string.IsNullOrEmpty(config.Role))
                    query["roleName"] = config.Role;
            }

            // Add required GUID parameters
            query["requestId"] = Guid.NewGuid().ToString();
            query["request_guid"] = Guid.NewGuid().ToString();

            uriBuilder.Query = query.ToString();
            return uriBuilder.ToString();
        }

        private class LoginResponse
        {
            public bool Success { get; set; }
            public string? Message { get; set; }
            public LoginData? Data { get; set; }
        }

        private class LoginData
        {
            public string? Token { get; set; }
            public string? SessionToken { get; set; }
            public long? SessionId { get; set; }
            public string? MasterToken { get; set; }
            public int MasterTokenValidityInSeconds { get; set; } = 14400; // Default 4 hours
        }
    }
}
