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
using FluentAssertions;
using NUnit.Framework;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    [TestFixture]
    public class SnowflakeDriverTests
    {
        private SnowflakeDriver _driver = null!;

        [SetUp]
        public void SetUp()
        {
            _driver = new SnowflakeDriver();
        }

        [TearDown]
        public void TearDown()
        {
            _driver?.Dispose();
        }

        [Test]
        public void Open_WithValidParameters_ShouldReturnDatabase()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                ["account"] = "testaccount",
                ["user"] = "testuser",
                ["password"] = "testpass"
            };

            // Act
            using var database = _driver.Open(parameters);

            // Assert
            database.Should().NotBeNull();
            database.Should().BeOfType<SnowflakeDatabase>();
        }

        [Test]
        public void Open_WithNullParameters_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() => _driver.Open((IReadOnlyDictionary<string, string>)null!));
            exception.ParamName.Should().Be("parameters");
        }

        [Test]
        public void Open_WithInvalidParameters_ShouldThrowArgumentException()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                ["invalid"] = "parameter"
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => _driver.Open(parameters));
            exception.Message.Should().Contain("Failed to parse connection parameters");
        }

        [Test]
        public void Open_WithMissingRequiredParameters_ShouldThrowArgumentException()
        {
            // Arrange
            var parameters = new Dictionary<string, string>
            {
                ["user"] = "testuser",
                ["password"] = "testpass"
                // Missing account
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => _driver.Open(parameters));
            exception.Message.Should().Contain("Failed to parse connection parameters");
        }

        [Test]
        public void Dispose_ShouldNotThrow()
        {
            // Act & Assert
            Assert.DoesNotThrow(() => _driver.Dispose());
        }

        [Test]
        public void Dispose_CalledMultipleTimes_ShouldNotThrow()
        {
            // Act & Assert
            Assert.DoesNotThrow(() =>
            {
                _driver.Dispose();
                _driver.Dispose();
            });
        }
    }
}