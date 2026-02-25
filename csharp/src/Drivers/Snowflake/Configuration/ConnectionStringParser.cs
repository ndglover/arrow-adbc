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
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Linq;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Configuration
{
    /// <summary>
    /// Parses ADBC-compliant connection strings into ConnectionConfig objects.
    /// </summary>
    public static class ConnectionStringParser
    {
        /// <summary>
        /// Parses a connection string into a ConnectionConfig object.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <returns>A configured ConnectionConfig object.</returns>
        /// <exception cref="ArgumentException">Thrown when the connection string is invalid.</exception>
        public static ConnectionConfig Parse(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
            }

            var parameters = ParseConnectionStringParameters(connectionString);
            var config = new ConnectionConfig();

            // Required parameters
            config.Account = GetRequiredParameter(parameters, "account");
            config.User = GetRequiredParameter(parameters, "user");

            // Optional connection parameters
            config.Database = GetOptionalParameter(parameters, "database");
            config.Schema = GetOptionalParameter(parameters, "schema");
            config.Warehouse = GetOptionalParameter(parameters, "warehouse");
            config.Role = GetOptionalParameter(parameters, "role");

            // Authentication configuration
            config.Authentication = ParseAuthenticationConfig(parameters);

            // Timeout configurations
            if (parameters.TryGetValue("query_timeout", out var queryTimeoutStr) &&
                int.TryParse(queryTimeoutStr, out var queryTimeoutSeconds))
            {
                config.QueryTimeout = TimeSpan.FromSeconds(queryTimeoutSeconds);
            }

            // Other configuration options
            if (parameters.TryGetValue("enable_compression", out var compressionStr) &&
                bool.TryParse(compressionStr, out var enableCompression))
            {
                config.EnableCompression = enableCompression;
            }

            // Connection pool configuration
            config.PoolConfig = ParseConnectionPoolConfig(parameters);

            // Validate the configuration
            ValidateConfiguration(config);

            return config;
        }

        private static Dictionary<string, string> ParseConnectionStringParameters(string connectionString)
        {
            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var pairs = connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var pair in pairs)
            {
                var equalIndex = pair.IndexOf('=');
                if (equalIndex <= 0 || equalIndex == pair.Length - 1)
                {
                    throw new ArgumentException($"Invalid connection string parameter: '{pair}'");
                }

                var key = pair.Substring(0, equalIndex).Trim();
                var value = pair.Substring(equalIndex + 1).Trim();

                // Support environment variable substitution
                value = ExpandEnvironmentVariables(value);

                parameters[key] = value;
            }

            return parameters;
        }

        private static string ExpandEnvironmentVariables(string value)
        {
            if (value.StartsWith("${") && value.EndsWith("}"))
            {
                var envVarName = value.Substring(2, value.Length - 3);
                var envValue = Environment.GetEnvironmentVariable(envVarName);
                if (envValue != null)
                {
                    return envValue;
                }
            }

            return value;
        }

        private static AuthenticationConfig ParseAuthenticationConfig(Dictionary<string, string> parameters)
        {
            var authConfig = new AuthenticationConfig();

            // Determine authentication type
            if (parameters.TryGetValue("authenticator", out var authenticatorStr))
            {
                authConfig.Type = authenticatorStr.ToLowerInvariant() switch
                {
                    "default" or "snowflake" => AuthenticationType.UsernamePassword,
                    "key_pair" or "jwt" => AuthenticationType.KeyPair,
                    "oauth" => AuthenticationType.OAuth,
                    "sso" => AuthenticationType.Sso,
                    "externalbrowser" => AuthenticationType.ExternalBrowser,
                    _ => throw new ArgumentException($"Unsupported authenticator: {authenticatorStr}")
                };
            }

            // Set authentication-specific parameters
            authConfig.Password = GetOptionalParameter(parameters, "password");
            authConfig.PrivateKeyPath = GetOptionalParameter(parameters, "private_key_path");
            authConfig.PrivateKeyPassphrase = GetOptionalParameter(parameters, "private_key_passphrase");
            authConfig.OAuthToken = GetOptionalParameter(parameters, "oauth_token");
            authConfig.OAuthRefreshToken = GetOptionalParameter(parameters, "oauth_refresh_token");

            // Parse SSO properties (any parameter starting with "sso_")
            foreach (var kvp in parameters.Where(p => p.Key.StartsWith("sso_", StringComparison.OrdinalIgnoreCase)))
            {
                var ssoKey = kvp.Key.Substring(4); // Remove "sso_" prefix
                authConfig.SsoProperties[ssoKey] = kvp.Value;
            }

            return authConfig;
        }

        private static ConnectionPoolConfig ParseConnectionPoolConfig(Dictionary<string, string> parameters)
        {
            var poolConfig = new ConnectionPoolConfig();

            if (parameters.TryGetValue("max_pool_size", out var maxPoolSizeStr) &&
                int.TryParse(maxPoolSizeStr, out var maxPoolSize))
            {
                poolConfig.MaxPoolSize = maxPoolSize;
            }

            if (parameters.TryGetValue("pool_idle_timeout", out var idleTimeoutStr) &&
                int.TryParse(idleTimeoutStr, out var idleTimeoutSeconds))
            {
                poolConfig.IdleTimeout = TimeSpan.FromSeconds(idleTimeoutSeconds);
            }

            if (parameters.TryGetValue("pool_max_lifetime", out var maxLifetimeStr) &&
                int.TryParse(maxLifetimeStr, out var maxLifetimeSeconds))
            {
                poolConfig.MaxConnectionLifetime = TimeSpan.FromSeconds(maxLifetimeSeconds);
            }

            return poolConfig;
        }

        private static string GetRequiredParameter(Dictionary<string, string> parameters, string key)
        {
            if (!parameters.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"Required parameter '{key}' is missing or empty.");
            }
            return value;
        }

        private static string? GetOptionalParameter(Dictionary<string, string> parameters, string key)
        {
            parameters.TryGetValue(key, out var value);
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        private static bool IsKnownParameter(string key)
        {
            var knownParameters = new[]
            {
                "account", "user", "password", "database", "schema", "warehouse", "role",
                "authenticator", "private_key_path", "private_key_passphrase",
                "oauth_token", "oauth_refresh_token",
                "query_timeout", "enable_compression",
                "max_pool_size", "pool_idle_timeout", "pool_max_lifetime"
            };

            return knownParameters.Contains(key, StringComparer.OrdinalIgnoreCase) ||
                   key.StartsWith("sso_", StringComparison.OrdinalIgnoreCase);
        }

        private static void ValidateConfiguration(ConnectionConfig config)
        {
            var validationResults = new List<ValidationResult>();
            var validationContext = new ValidationContext(config);

            // Validate the main configuration
            Validator.TryValidateObject(config, validationContext, validationResults, true);

            // Validate authentication configuration
            var authValidationResults = config.Authentication.Validate();
            validationResults.AddRange(authValidationResults);

            // Validate pool configuration
            var poolValidationContext = new ValidationContext(config.PoolConfig);
            Validator.TryValidateObject(config.PoolConfig, poolValidationContext, validationResults, true);

            if (validationResults.Any())
            {
                var errorMessages = validationResults.Select(vr => vr.ErrorMessage).ToArray();
                throw new ArgumentException($"Configuration validation failed: {string.Join("; ", errorMessages)}");
            }
        }
    }
}