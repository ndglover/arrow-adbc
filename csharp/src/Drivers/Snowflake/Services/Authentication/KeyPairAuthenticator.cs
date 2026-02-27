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
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

/// <summary>
/// Implements RSA key pair authentication for Snowflake.
/// </summary>
public class KeyPairAuthenticator : IKeyPairAuthenticator
{
    private readonly HttpClient _httpClient;
    private const string LoginEndpoint = "/session/v1/login-request";

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyPairAuthenticator"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client for making requests.</param>
    public KeyPairAuthenticator(HttpClient httpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        string account,
        string user,
        string privateKeyPath,
        string? privateKeyPassphrase = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(account))
            throw new ArgumentException("Account cannot be null or empty.", nameof(account));
        
        if (string.IsNullOrEmpty(user))
            throw new ArgumentException("User cannot be null or empty.", nameof(user));
        
        if (string.IsNullOrEmpty(privateKeyPath))
            throw new ArgumentException("Private key path cannot be null or empty.", nameof(privateKeyPath));

        if (!File.Exists(privateKeyPath))
            throw new FileNotFoundException($"Private key file not found: {privateKeyPath}");

        // Load and process the private key
        var privateKeyPem = await File.ReadAllTextAsync(privateKeyPath, cancellationToken);
        var jwtToken = GenerateJwtToken(account, user, privateKeyPem, privateKeyPassphrase);

        var loginUrl = BuildLoginUrl(account);
        var loginRequest = new
        {
            data = new
            {
                ACCOUNT_NAME = account,
                LOGIN_NAME = user,
                AUTHENTICATOR = "SNOWFLAKE_JWT",
                TOKEN = jwtToken,
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
                throw new InvalidOperationException($"Snowflake key pair authentication failed: {errorMessage}");
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
            throw new InvalidOperationException($"Failed to authenticate with Snowflake using key pair: {ex.Message}", ex);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to parse Snowflake authentication response: {ex.Message}", ex);
        }
    }

    private static string GenerateJwtToken(string account, string user, string privateKeyPem, string? passphrase)
    {
        try
        {
            // Parse the private key
            using var rsa = RSA.Create();
            
            if (!string.IsNullOrEmpty(passphrase))
            {
                rsa.ImportFromEncryptedPem(privateKeyPem, passphrase);
            }
            else
            {
                rsa.ImportFromPem(privateKeyPem);
            }

            // Generate public key fingerprint (SHA256 hash of public key)
            var publicKey = rsa.ExportSubjectPublicKeyInfo();
            var publicKeyFingerprint = Convert.ToBase64String(SHA256.HashData(publicKey));

            // Create JWT header
            var header = new
            {
                alg = "RS256",
                typ = "JWT"
            };

            // Create JWT payload
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var payload = new
            {
                iss = $"{account.ToUpperInvariant()}.{user.ToUpperInvariant()}.{publicKeyFingerprint}",
                sub = $"{account.ToUpperInvariant()}.{user.ToUpperInvariant()}",
                iat = now,
                exp = now + 3600 // 1 hour expiration
            };

            // Encode header and payload
            var headerJson = JsonSerializer.Serialize(header);
            var payloadJson = JsonSerializer.Serialize(payload);
            
            var headerBase64 = Base64UrlEncode(Encoding.UTF8.GetBytes(headerJson));
            var payloadBase64 = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
            
            var signatureInput = $"{headerBase64}.{payloadBase64}";
            var signatureBytes = rsa.SignData(
                Encoding.UTF8.GetBytes(signatureInput),
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            
            var signatureBase64 = Base64UrlEncode(signatureBytes);
            
            return $"{signatureInput}.{signatureBase64}";
        }
        catch (CryptographicException ex)
        {
            throw new InvalidOperationException($"Failed to process private key: {ex.Message}", ex);
        }
    }

    private static string Base64UrlEncode(byte[] input)
    {
        return Convert.ToBase64String(input)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
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
        public int MasterTokenValidityInSeconds { get; set; } = 14400;
    }
}
