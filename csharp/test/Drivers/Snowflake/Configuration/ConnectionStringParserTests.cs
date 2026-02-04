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
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake.Configuration
{
    public class ConnectionStringParserTests
    {
        [Fact]
        public void Parse_WithValidBasicConnectionString_ShouldReturnValidConfig()
        {
            // Arrange
            var connectionString = "account=testaccount;user=testuser;password=testpass;database=testdb";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

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
            var connectionString = "account=testaccount;user=testuser;authenticator=key_pair;private_key_path=/path/to/key.pem";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

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
            var connectionString = "account=testaccount;user=testuser;authenticator=oauth;oauth_token=test_token";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

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
            var connectionString = "account=testaccount;user=testuser;password=testpass;connection_timeout=60;query_timeout=300";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

            // Assert
            Assert.NotNull(config);
            Assert.Equal(TimeSpan.FromSeconds(60), config.ConnectionTimeout);
            Assert.Equal(TimeSpan.FromSeconds(300), config.QueryTimeout);
        }

        [Fact]
        public void Parse_WithPoolConfiguration_ShouldReturnValidConfig()
        {
            // Arrange
            var connectionString = "account=testaccount;user=testuser;password=testpass;max_pool_size=20;min_pool_size=2";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

            // Assert
            Assert.NotNull(config);
            Assert.Equal(20, config.PoolConfig.MaxPoolSize);
            Assert.Equal(2, config.PoolConfig.MinPoolSize);
        }

        [Fact]
        public void Parse_WithSsoProperties_ShouldReturnValidConfig()
        {
            // Arrange
            var connectionString = "account=testaccount;user=testuser;authenticator=sso;sso_url=https://sso.example.com;sso_provider=okta";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

            // Assert
            Assert.NotNull(config);
            Assert.Equal(AuthenticationType.Sso, config.Authentication.Type);
            Assert.True(config.Authentication.SsoProperties.ContainsKey("url"));
            Assert.True(config.Authentication.SsoProperties.ContainsKey("provider"));
            Assert.Equal("https://sso.example.com", config.Authentication.SsoProperties["url"]);
            Assert.Equal("okta", config.Authentication.SsoProperties["provider"]);
        }

        [Fact]
        public void Parse_WithAdditionalProperties_ShouldStoreInAdditionalProperties()
        {
            // Arrange
            var connectionString = "account=testaccount;user=testuser;password=testpass;custom_property=custom_value";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

            // Assert
            Assert.NotNull(config);
            Assert.True(config.AdditionalProperties.ContainsKey("custom_property"));
            Assert.Equal("custom_value", config.AdditionalProperties["custom_property"]);
        }

        [Fact]
        public void Parse_WithNullConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse(null!));
            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Parse_WithEmptyConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse(""));
            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Parse_WithWhitespaceConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse("   "));
            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Parse_WithMissingRequiredParameter_ShouldThrowArgumentException()
        {
            // Arrange
            var connectionString = "user=testuser;password=testpass"; // Missing account

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse(connectionString));
            Assert.Contains("account", exception.Message);
        }

        [Fact]
        public void Parse_WithInvalidParameterFormat_ShouldThrowArgumentException()
        {
            // Arrange
            var connectionString = "account=testaccount;invalid_parameter_without_value;user=testuser";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse(connectionString));
        }

        [Fact]
        public void Parse_WithInvalidAuthenticator_ShouldThrowArgumentException()
        {
            // Arrange
            var connectionString = "account=testaccount;user=testuser;authenticator=invalid_auth";

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.Parse(connectionString));
            Assert.Contains("Unsupported authenticator", exception.Message);
        }

        [Fact]
        public void Parse_WithCaseInsensitiveParameters_ShouldReturnValidConfig()
        {
            // Arrange
            var connectionString = "ACCOUNT=testaccount;User=testuser;PASSWORD=testpass;DATABASE=testdb";

            // Act
            var config = ConnectionStringParser.Parse(connectionString);

            // Assert
            Assert.NotNull(config);
            Assert.Equal("testaccount", config.Account);
            Assert.Equal("testuser", config.User);
            Assert.Equal("testdb", config.Database);
            Assert.Equal("testpass", config.Authentication.Password);
        }

        [Fact]
        public void Parse_WithEnvironmentVariableSubstitution_ShouldExpandVariables()
        {
            // Arrange
            Environment.SetEnvironmentVariable("TEST_PASSWORD", "env_password");
            var connectionString = "account=testaccount;user=testuser;password=${TEST_PASSWORD}";

            try
            {
                // Act
                var config = ConnectionStringParser.Parse(connectionString);

                // Assert
                Assert.NotNull(config);
                Assert.Equal("env_password", config.Authentication.Password);
            }
            finally
            {
                Environment.SetEnvironmentVariable("TEST_PASSWORD", null);
            }
        }
    }
}