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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

/// <summary>
/// Parses ADBC parameters into ConnectionConfig objects.
/// </summary>
public static class ConnectionStringParser
{
    /// <summary>
    /// Parses ADBC parameters with connection-specific overrides into a ConnectionConfig object.
    /// Connection parameters take precedence over database defaults.
    /// </summary>
    /// <param name="connectionParameters">Connection-specific parameters (take precedence).</param>
    /// <param name="databaseDefaults">Database default parameters.</param>
    /// <returns>A configured ConnectionConfig object with merged parameters.</returns>
    /// <exception cref="ArgumentException">Thrown when the parameters are invalid.</exception>
    public static ConnectionConfig ParseParameters(
        IReadOnlyDictionary<string, string>? connectionParameters = null,
        IReadOnlyDictionary<string, string>? databaseDefaults = null)
    {
        // If both are null, create empty dictionary (will fail validation)
        if (connectionParameters == null && databaseDefaults == null)
        {
            return BuildConfig(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
        }

        // If only one is provided, use it directly
        if (databaseDefaults == null)
        {
            return BuildConfig(new Dictionary<string, string>(connectionParameters!, StringComparer.OrdinalIgnoreCase));
        }

        if (connectionParameters == null)
        {
            return BuildConfig(new Dictionary<string, string>(databaseDefaults, StringComparer.OrdinalIgnoreCase));
        }

        // Both provided - merge with connection parameters taking precedence
        var merged = new Dictionary<string, string>(connectionParameters, StringComparer.OrdinalIgnoreCase);
        
        foreach (var kvp in databaseDefaults)
        {
            if (!merged.ContainsKey(kvp.Key))
            {
                merged[kvp.Key] = kvp.Value;
            }
        }
        
        return BuildConfig(merged);
    }

    private static ConnectionConfig BuildConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var config = new ConnectionConfig
        {
            Account = GetRequiredParameter(parameters, "account"),
            User = GetRequiredParameter(parameters, "user"),
            Database = GetOptionalParameter(parameters, "db") ?? GetOptionalParameter(parameters, "database"),
            Schema = GetOptionalParameter(parameters, "schema"),
            Warehouse = GetOptionalParameter(parameters, "warehouse"),
            Role = GetOptionalParameter(parameters, "role"),
            Authentication = ParseAuthenticationConfig(parameters)
        };

        if (parameters.TryGetValue("connection_timeout", out string? connectionTimeoutStr) &&
            int.TryParse(connectionTimeoutStr, out int connectionTimeoutSeconds))
        {
            config.QueryTimeout = TimeSpan.FromSeconds(connectionTimeoutSeconds);
        }

        if (parameters.TryGetValue("enable_compression", out string? compressionStr) &&
            bool.TryParse(compressionStr, out bool enableCompression))
        {
            config.EnableCompression = enableCompression;
        }

        config.PoolConfig = ParseConnectionPoolConfig(parameters);

        ValidateConfiguration(config);

        return config;
    }

    private static AuthenticationConfig ParseAuthenticationConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var authConfig = new AuthenticationConfig();
        if (parameters.TryGetValue("authenticator", out var authenticatorStr))
        {
            authConfig.Type = authenticatorStr.ToLowerInvariant() switch
            {
                "default" or "snowflake" => AuthenticationType.UsernamePassword,
                "key_pair" or "jwt" or "snowflake_jwt" => AuthenticationType.KeyPair,
                "oauth" => AuthenticationType.OAuth,
                "sso" => AuthenticationType.Sso,
                "externalbrowser" => AuthenticationType.ExternalBrowser,
                _ => throw new ArgumentException($"Unsupported authenticator: {authenticatorStr}")
            };
        }

        authConfig.Password = GetOptionalParameter(parameters, "password");
        authConfig.PrivateKeyPath = GetOptionalParameter(parameters, "private_key_file") ?? GetOptionalParameter(parameters, "private_key_path");
        authConfig.PrivateKeyPassphrase = GetOptionalParameter(parameters, "private_key_pwd") ?? GetOptionalParameter(parameters, "private_key_passphrase");
        authConfig.OAuthToken = GetOptionalParameter(parameters, "token") ?? GetOptionalParameter(parameters, "oauth_token");
        authConfig.OAuthRefreshToken = GetOptionalParameter(parameters, "oauth_refresh_token");

        foreach (var kvp in parameters.Where(p => p.Key.StartsWith("sso_", StringComparison.OrdinalIgnoreCase)))
            authConfig.SsoProperties[kvp.Key[4..]] = kvp.Value;

        return authConfig;
    }

    private static ConnectionPoolConfig ParseConnectionPoolConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var poolConfig = new ConnectionPoolConfig();

        if ((parameters.TryGetValue("maxpoolsize", out string? maxPoolSizeStr) || parameters.TryGetValue("max_pool_size", out maxPoolSizeStr)) &&
            int.TryParse(maxPoolSizeStr, out int maxPoolSize))
        {
            poolConfig.MaxPoolSize = maxPoolSize;
        }

        if ((parameters.TryGetValue("waitingforidlesessiontimeout", out string? idleTimeoutStr) || parameters.TryGetValue("pool_idle_timeout", out idleTimeoutStr)))
        {
            poolConfig.IdleTimeout = ParseTimeSpan(idleTimeoutStr);
        }

        if ((parameters.TryGetValue("expirationtimeout", out string? maxLifetimeStr) || parameters.TryGetValue("pool_max_lifetime", out maxLifetimeStr)))
        {
            poolConfig.MaxConnectionLifetime = ParseTimeSpan(maxLifetimeStr);
        }

        return poolConfig;
    }

    private static TimeSpan ParseTimeSpan(string value)
    {
        // Support Snowflake format (e.g., "30s", "60m") and plain seconds
        if (int.TryParse(value, out int seconds))
            return TimeSpan.FromSeconds(seconds);

        if (value.EndsWith("s", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int s))
            {
                return TimeSpan.FromSeconds(s);
            }
        }
        else if (value.EndsWith("m", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int m))
            {
                return TimeSpan.FromMinutes(m);
            }
        }
        else if (value.EndsWith("h", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int h))
            {
                return TimeSpan.FromHours(h);
            }
        }

        throw new ArgumentException($"Invalid timespan format: {value}. Expected format: number with optional suffix (s, m, h) or plain seconds.");
    }

    private static string GetRequiredParameter(IReadOnlyDictionary<string, string> parameters, string key)
    {
        if (!parameters.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"Required parameter '{key}' is missing or empty.");
        }
        return value;
    }

    private static string? GetOptionalParameter(IReadOnlyDictionary<string, string> parameters, string key)
    {
        parameters.TryGetValue(key, out string? value);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private static void ValidateConfiguration(ConnectionConfig config)
    {
        var validationResults = new List<ValidationResult>();
        var validationContext = new ValidationContext(config);

        Validator.TryValidateObject(config, validationContext, validationResults, true);

        var authValidationResults = config.Authentication.Validate();
        validationResults.AddRange(authValidationResults);

        var poolValidationContext = new ValidationContext(config.PoolConfig);
        Validator.TryValidateObject(config.PoolConfig, poolValidationContext, validationResults, true);

        if (!validationResults.Any())
            return;

        var errorMessages = validationResults.Select(vr => vr.ErrorMessage).ToArray();
        throw new ArgumentException($"Configuration validation failed: {string.Join("; ", errorMessages)}");
    }
}
