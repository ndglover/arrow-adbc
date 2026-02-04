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
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake.Configuration;

public class ConnectionStringParserTests
{
    private static Dictionary<string, string> ParseConnectionString(string connectionString)
    {
        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var pairs = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        
        foreach (var pair in pairs)
        {
            var parts = pair.Split('=', 2);
            if (parts.Length == 2)
            {
                parameters[parts[0].Trim()] = parts[1].Trim();
            }
        }
        
        return parameters;
    }

    [Fact]
    public void Parse_WithValidBasicConnectionString_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;password=testpass;database=testdb");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("testdb", config.Database);
        Assert.Equal(AuthenticationType.UsernamePassword, config.Authentication.Type);
        Assert.Equal("testpass", config.Authentication.Password);
    }

    [Fact]
    public void Parse_WithKeyPairAuthentication_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;authenticator=key_pair;private_key_path=/path/to/key.pem");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal(AuthenticationType.KeyPair, config.Authentication.Type);
        Assert.Equal("/path/to/key.pem", config.Authentication.PrivateKeyPath);
    }

    [Fact]
    public void Parse_WithOAuthAuthentication_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;authenticator=oauth;oauth_token=test_token");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal(AuthenticationType.OAuth, config.Authentication.Type);
        Assert.Equal("test_token", config.Authentication.OAuthToken);
    }

    [Fact]
    public void Parse_WithTimeoutSettings_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;password=testpass;connection_timeout=300");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal(TimeSpan.FromSeconds(300), config.QueryTimeout);
    }

    [Fact]
    public void Parse_WithPoolConfiguration_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;password=testpass;max_pool_size=20");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal(20, config.PoolConfig.MaxPoolSize);
    }

    [Fact]
    public void Parse_WithSsoProperties_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;authenticator=sso;sso_url=https://sso.example.com;sso_provider=okta");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal(AuthenticationType.Sso, config.Authentication.Type);
        Assert.True(config.Authentication.SsoProperties.ContainsKey("url"));
        Assert.True(config.Authentication.SsoProperties.ContainsKey("provider"));
        Assert.Equal("https://sso.example.com", config.Authentication.SsoProperties["url"]);
        Assert.Equal("okta", config.Authentication.SsoProperties["provider"]);
    }

    [Fact]
    public void Parse_WithNullParameters_ShouldThrowArgumentException()
    {
        // Act & Assert - null parameters result in empty dictionary which fails validation
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(null));
        Assert.Contains("account", exception.Message);
    }

    [Fact]
    public void Parse_WithMissingRequiredParameter_ShouldThrowArgumentException()
    {
        // Arrange
        var parameters = ParseConnectionString("user=testuser;password=testpass"); // Missing account

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(parameters));
        Assert.Contains("account", exception.Message);
    }

    [Fact]
    public void Parse_WithInvalidAuthenticator_ShouldThrowArgumentException()
    {
        // Arrange
        var parameters = ParseConnectionString("account=testaccount;user=testuser;authenticator=invalid_auth");

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(parameters));
        Assert.Contains("Unsupported authenticator", exception.Message);
    }

    [Fact]
    public void Parse_WithCaseInsensitiveParameters_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("ACCOUNT=testaccount;User=testuser;PASSWORD=testpass;DATABASE=testdb");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("testdb", config.Database);
        Assert.Equal("testpass", config.Authentication.Password);
    }

    [Fact]
    public void ParseParameters_WithConnectionOverrides_ShouldMergeCorrectly()
    {
        // Arrange - database parameters
        var databaseParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "account", "testaccount" },
            { "user", "testuser" },
            { "password", "testpass" },
            { "warehouse", "DEFAULT_WH" },
            { "database", "DEFAULT_DB" }
        };

        // Connection-specific overrides
        var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "warehouse", "ANALYTICS_WH" },  // Override
            { "schema", "PUBLIC" }  // New parameter
        };

        // Act
        var config = ConnectionStringParser.ParseParameters(connectionParams, databaseParams);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("ANALYTICS_WH", config.Warehouse); // Overridden value
        Assert.Equal("DEFAULT_DB", config.Database); // From database params
        Assert.Equal("PUBLIC", config.Schema); // From connection params
    }

    [Fact]
    public void ParseParameters_WithCaseInsensitiveMerge_ShouldHandleCorrectly()
    {
        // Arrange - test that case-insensitive merge works correctly
        var databaseParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "Account", "testaccount" },
            { "User", "testuser" },
            { "Warehouse", "DEFAULT_WH" }
        };

        var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "warehouse", "OVERRIDE_WH" },  // Different casing, should override
            { "password", "testpass" }
        };

        // Act
        var config = ConnectionStringParser.ParseParameters(connectionParams, databaseParams);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("OVERRIDE_WH", config.Warehouse); // Should use connection override despite case difference
        Assert.Equal("testpass", config.Authentication.Password);
    }
}
