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

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    internal class SnowflakeTestingUtils
    {
        internal static readonly SnowflakeTestConfiguration TestConfiguration;

        internal const string SNOWFLAKE_TEST_CONFIG_VARIABLE = "SNOWFLAKE_TEST_CONFIG_FILE";

        static SnowflakeTestingUtils()
        {
            try
            {
                TestConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SNOWFLAKE_TEST_CONFIG_VARIABLE);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"Cannot load test configuration from environment variable `{SNOWFLAKE_TEST_CONFIG_VARIABLE}`");
                Console.WriteLine(ex.Message);
                TestConfiguration = new SnowflakeTestConfiguration();
            }
        }

        /// <summary>
        /// Gets the native Snowflake ADBC driver with settings from the
        /// <see cref="SnowflakeTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetSnowflakeAdbcDriver(
            SnowflakeTestConfiguration testConfiguration,
            out Dictionary<string, string> parameters)
        {
            parameters = new Dictionary<string, string>
            {
                { "account", Parameter(testConfiguration.Account, "account") },
                { "user", Parameter(testConfiguration.User, "user") }
            };

            // Add authentication
            if (testConfiguration.Authentication.Default is not null)
            {
                parameters["user"] = Parameter(testConfiguration.Authentication.Default.User, "user");
                parameters["password"] = Parameter(testConfiguration.Authentication.Default.Password, "password");
            }
            else if (testConfiguration.Authentication.SnowflakeJwt is not null)
            {
                parameters["user"] = Parameter(testConfiguration.Authentication.SnowflakeJwt.User, "user");
                parameters["authenticator"] = "jwt";
                
                if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile))
                {
                    parameters["private_key_path"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile;
                }
                else if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKey))
                {
                    parameters["private_key"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKey;
                }

                if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase))
                {
                    parameters["private_key_passphrase"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase;
                }
            }
            else if (testConfiguration.Authentication.OAuth is not null)
            {
                parameters["user"] = Parameter(testConfiguration.Authentication.OAuth.User, "user");
                parameters["authenticator"] = "oauth";
                parameters["oauth_token"] = Parameter(testConfiguration.Authentication.OAuth.Token, "oauth_token");
            }
            else
            {
                // Fallback to top-level user/password
                parameters["password"] = Parameter(testConfiguration.Password, "password");
            }

            // Add optional parameters
            if (!string.IsNullOrWhiteSpace(testConfiguration.Database))
            {
                parameters["database"] = testConfiguration.Database;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Schema))
            {
                parameters["schema"] = testConfiguration.Schema;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Warehouse))
            {
                parameters["warehouse"] = testConfiguration.Warehouse;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Role))
            {
                parameters["role"] = testConfiguration.Role;
            }

            return new SnowflakeDriver();
        }

        private static string Parameter(string? value, string parameterName)
        {
            if (value == null) throw new ArgumentNullException(parameterName);
            return value;
        }
    }
}
