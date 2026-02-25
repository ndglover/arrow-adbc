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

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication
{
    /// <summary>
    /// Provides RSA key pair authentication for Snowflake.
    /// </summary>
    public interface IKeyPairAuthenticator
    {
        /// <summary>
        /// Authenticates using RSA key pair.
        /// </summary>
        /// <param name="account">The Snowflake account identifier.</param>
        /// <param name="user">The username.</param>
        /// <param name="privateKeyPath">The path to the private key file.</param>
        /// <param name="privateKeyPassphrase">The passphrase for encrypted private keys (optional).</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An authentication token.</returns>
        Task<AuthenticationToken> AuthenticateAsync(
            string account,
            string user,
            string privateKeyPath,
            string? privateKeyPassphrase = null,
            CancellationToken cancellationToken = default);
    }
}
