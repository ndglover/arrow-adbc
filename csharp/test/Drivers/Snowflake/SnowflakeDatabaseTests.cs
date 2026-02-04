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
using Apache.Arrow.Adbc.Drivers.Snowflake;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;
using FluentAssertions;
using NUnit.Framework;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    [TestFixture]
    public class SnowflakeDatabaseTests
    {
        private ConnectionConfig _config = null!;
        private SnowflakeDatabase _database = null!;

        [SetUp]
        public void SetUp()
        {
            _config = new ConnectionConfig
            {
                Account = "testaccount",
                User = "testuser",
                Authentication = new AuthenticationConfig
                {
                    Type = AuthenticationType.UsernamePassword,
                    Password = "testpass"
                }
            };
            _database = new SnowflakeDatabase(_config);
        }

        [TearDown]
        public void TearDown()
        {
            _database?.Dispose();
        }

        [Test]
        public void Constructor_WithNullConfig_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new SnowflakeDatabase(null!));
        }

        [Test]
        public void Connect_ShouldReturnConnection()
        {
            // Act
            using var connection = _database.Connect(null);

            // Assert
            connection.Should().NotBeNull();
            connection.Should().BeOfType<SnowflakeConnection>();
        }

        [Test]
        public void Connect_WithParameters_ShouldReturnConnection()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                ["warehouse"] = "test_warehouse"
            };

            // Act
            using var connection = _database.Connect(parameters);

            // Assert
            connection.Should().NotBeNull();
            connection.Should().BeOfType<SnowflakeConnection>();
        }

        [Test]
        public void Connect_AfterDispose_ShouldThrowObjectDisposedException()
        {
            // Arrange
            _database.Dispose();

            // Act & Assert
            Assert.Throws<ObjectDisposedException>(() => _database.Connect(null));
        }

        [Test]
        public void Dispose_ShouldNotThrow()
        {
            // Act & Assert
            Assert.DoesNotThrow(() => _database.Dispose());
        }

        [Test]
        public void Dispose_CalledMultipleTimes_ShouldNotThrow()
        {
            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                _database.Dispose();
                _database.Dispose();
            });
        }
    }
}