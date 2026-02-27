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
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

/// <summary>
/// Implements Single Sign-On (SSO) authentication for Snowflake using external browser.
/// </summary>
public class SsoAuthenticator : ISsoAuthenticator
{
    private readonly HttpClient _httpClient;
    private const string LoginEndpoint = "/session/v1/login-request";
    private const string AuthenticatorEndpoint = "/session/authenticator-request";

    /// <summary>
    /// Initializes a new instance of the <see cref="SsoAuthenticator"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client for making requests.</param>
    public SsoAuthenticator(HttpClient httpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        string account,
        string user,
        Dictionary<string, string>? ssoProperties = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(account))
            throw new ArgumentException("Account cannot be null or empty.", nameof(account));
        
        if (string.IsNullOrEmpty(user))
            throw new ArgumentException("User cannot be null or empty.", nameof(user));

        // Step 1: Get SSO URL from Snowflake
        var ssoUrl = await GetSsoUrlAsync(account, user, ssoProperties, cancellationToken);

        // Step 2: Open browser for user authentication
        var samlResponse = await AuthenticateWithBrowserAsync(ssoUrl, cancellationToken);

        // Step 3: Complete authentication with SAML response
        return await CompleteAuthenticationAsync(account, user, samlResponse, cancellationToken);
    }

    private async Task<string> GetSsoUrlAsync(
        string account,
        string user,
        Dictionary<string, string>? ssoProperties,
        CancellationToken cancellationToken)
    {
        var authenticatorUrl = BuildAuthenticatorUrl(account);
        var authenticatorRequest = new
        {
            data = new
            {
                ACCOUNT_NAME = account,
                LOGIN_NAME = user,
                AUTHENTICATOR = "EXTERNALBROWSER",
                CLIENT_APP_ID = "ADBC",
                CLIENT_APP_VERSION = "1.0.0"
            }
        };

        try
        {
            var response = await _httpClient.PostAsJsonAsync(authenticatorUrl, authenticatorRequest, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadFromJsonAsync<AuthenticatorResponse>(cancellationToken);
            
            if (responseContent?.Data?.SsoUrl == null)
                throw new InvalidOperationException("Failed to retrieve SSO URL from Snowflake.");

            return responseContent.Data.SsoUrl;
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException($"Failed to get SSO URL from Snowflake: {ex.Message}", ex);
        }
    }

    private async Task<string> AuthenticateWithBrowserAsync(string ssoUrl, CancellationToken cancellationToken)
    {
        // Create a local HTTP listener to receive the SAML response
        using var listener = new HttpListener();
        var callbackPort = 8080;
        var callbackUrl = $"http://localhost:{callbackPort}/";
        listener.Prefixes.Add(callbackUrl);
        
        try
        {
            listener.Start();

            // Open the SSO URL in the default browser
            OpenBrowser(ssoUrl);

            // Wait for the callback with SAML response
            var context = await listener.GetContextAsync();
            var samlResponse = context.Request.QueryString["SAMLResponse"];

            if (string.IsNullOrEmpty(samlResponse))
                throw new InvalidOperationException("No SAML response received from SSO authentication.");

            // Send success response to browser
            var responseBytes = System.Text.Encoding.UTF8.GetBytes(
                "<html><body><h1>Authentication Successful</h1><p>You can close this window.</p></body></html>");
            context.Response.ContentType = "text/html";
            context.Response.ContentLength64 = responseBytes.Length;
            await context.Response.OutputStream.WriteAsync(responseBytes, cancellationToken);
            context.Response.Close();

            return samlResponse;
        }
        finally
        {
            listener.Stop();
        }
    }

    private async Task<AuthenticationToken> CompleteAuthenticationAsync(
        string account,
        string user,
        string samlResponse,
        CancellationToken cancellationToken)
    {
        var loginUrl = BuildLoginUrl(account);
        var loginRequest = new
        {
            data = new
            {
                ACCOUNT_NAME = account,
                LOGIN_NAME = user,
                AUTHENTICATOR = "EXTERNALBROWSER",
                RAW_SAML_RESPONSE = samlResponse,
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
                throw new InvalidOperationException($"Snowflake SSO authentication failed: {errorMessage}");
            }

            return new AuthenticationToken
            {
                AccessToken = responseContent.Data.Token ?? throw new InvalidOperationException("No token received from Snowflake."),
                SessionToken = responseContent.Data.SessionToken,
                MasterToken = responseContent.Data.MasterToken,
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.Data.MasterTokenValidityInSeconds),
                TokenType = "Snowflake"
            };
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException($"Failed to complete SSO authentication: {ex.Message}", ex);
        }
    }

    private static void OpenBrowser(string url)
    {
        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = url,
                UseShellExecute = true
            });
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to open browser for SSO authentication: {ex.Message}", ex);
        }
    }

    private static string BuildLoginUrl(string account)
    {
        var accountUrl = account.Contains(".")
            ? $"https://{account}"
            : $"https://{account}.snowflakecomputing.com";
        
        return $"{accountUrl}{LoginEndpoint}";
    }

    private static string BuildAuthenticatorUrl(string account)
    {
        var accountUrl = account.Contains(".")
            ? $"https://{account}"
            : $"https://{account}.snowflakecomputing.com";
        
        return $"{accountUrl}{AuthenticatorEndpoint}";
    }

    private class AuthenticatorResponse
    {
        public bool Success { get; set; }
        public AuthenticatorData? Data { get; set; }
    }

    private class AuthenticatorData
    {
        public string? SsoUrl { get; set; }
        public string? ProofKey { get; set; }
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
        public int MasterTokenValidityInSeconds { get; set; } = 14400;
    }
}
