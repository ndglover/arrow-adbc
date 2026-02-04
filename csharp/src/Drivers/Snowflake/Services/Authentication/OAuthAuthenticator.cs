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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

/// <summary>
/// Implements OAuth 2.0 authentication for Snowflake.
/// </summary>
public class OAuthAuthenticator : IOAuthAuthenticator
{
    private readonly HttpClient _httpClient;
    private const string LoginEndpoint = "/session/v1/login-request";
    private const string TokenEndpoint = "/oauth/token-request";

    /// <summary>
    /// Initializes a new instance of the <see cref="OAuthAuthenticator"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client for making requests.</param>
    public OAuthAuthenticator(HttpClient httpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        string account,
        string user,
        string oauthToken,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(account))
            throw new ArgumentException("Account cannot be null or empty.", nameof(account));
        
        if (string.IsNullOrEmpty(user))
            throw new ArgumentException("User cannot be null or empty.", nameof(user));
        
        if (string.IsNullOrEmpty(oauthToken))
            throw new ArgumentException("OAuth token cannot be null or empty.", nameof(oauthToken));

        var loginUrl = BuildLoginUrl(account);
        var loginRequest = new
        {
            data = new
            {
                ACCOUNT_NAME = account,
                LOGIN_NAME = user,
                AUTHENTICATOR = "OAUTH",
                TOKEN = oauthToken,
                CLIENT_APP_ID = "ADBC",
                CLIENT_APP_VERSION = "1.0.0"
            }
        };

        try
        {
            var response = await _httpClient.PostAsJsonAsync(loginUrl, loginRequest, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadFromJsonAsync<LoginResponse>(cancellationToken);
            
            if (responseContent?.Data == null)
                throw new InvalidOperationException("Invalid response from Snowflake authentication service.");

            if (!responseContent.Success)
            {
                var errorMessage = responseContent.Message ?? "Authentication failed.";
                throw new InvalidOperationException($"Snowflake OAuth authentication failed: {errorMessage}");
            }

            return new AuthenticationToken
            {
                AccessToken = responseContent.Data.Token ?? throw new InvalidOperationException("No token received from Snowflake."),
                SessionToken = responseContent.Data.SessionToken,
                MasterToken = responseContent.Data.MasterToken,
                RefreshToken = responseContent.Data.RefreshToken,
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.Data.MasterTokenValidityInSeconds),
                TokenType = "Bearer"
            };
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException($"Failed to authenticate with Snowflake using OAuth: {ex.Message}", ex);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to parse Snowflake authentication response: {ex.Message}", ex);
        }
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> RefreshTokenAsync(
        string refreshToken,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(refreshToken))
            throw new ArgumentException("Refresh token cannot be null or empty.", nameof(refreshToken));

        // Note: Snowflake OAuth token refresh typically goes through the OAuth provider
        // This is a simplified implementation that may need adjustment based on the OAuth provider
        var tokenRequest = new
        {
            grant_type = "refresh_token",
            refresh_token = refreshToken
        };

        try
        {
            var response = await _httpClient.PostAsJsonAsync(TokenEndpoint, tokenRequest, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken);
            
            if (responseContent == null)
                throw new InvalidOperationException("Invalid response from OAuth token refresh.");

            return new AuthenticationToken
            {
                AccessToken = responseContent.AccessToken ?? throw new InvalidOperationException("No access token received."),
                RefreshToken = responseContent.RefreshToken ?? refreshToken,
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.ExpiresIn),
                TokenType = responseContent.TokenType ?? "Bearer"
            };
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException($"Failed to refresh OAuth token: {ex.Message}", ex);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to parse OAuth token refresh response: {ex.Message}", ex);
        }
    }

    private static string BuildLoginUrl(string account)
    {
        var accountUrl = account.Contains(".")
            ? $"https://{account}"
            : $"https://{account}.snowflakecomputing.com";
        
        return $"{accountUrl}{LoginEndpoint}";
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
        public string? MasterToken { get; set; }
        public string? RefreshToken { get; set; }
        public int MasterTokenValidityInSeconds { get; set; } = 14400;
    }

    private class TokenResponse
    {
        public string? AccessToken { get; set; }
        public string? RefreshToken { get; set; }
        public string? TokenType { get; set; }
        public int ExpiresIn { get; set; } = 3600;
    }
}
