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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services
{
    /// <summary>
    /// Provides authentication services for Snowflake connections.
    /// </summary>
    public interface IAuthenticationService
    {
        /// <summary>
        /// Authenticates using the provided configuration and returns an authentication token.
        /// </summary>
        /// <param name="config">The authentication configuration.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An authentication token.</returns>
        Task<AuthenticationToken> AuthenticateAsync(AuthenticationConfig config, CancellationToken cancellationToken = default);

        /// <summary>
        /// Refreshes an existing authentication token.
        /// </summary>
        /// <param name="token">The token to refresh.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A refreshed authentication token.</returns>
        Task<AuthenticationToken> RefreshTokenAsync(AuthenticationToken token, CancellationToken cancellationToken = default);

        /// <summary>
        /// Invalidates an authentication token.
        /// </summary>
        /// <param name="token">The token to invalidate.</param>
        void InvalidateToken(AuthenticationToken token);
    }

    /// <summary>
    /// Represents an authentication token for Snowflake connections.
    /// </summary>
    public class AuthenticationToken
    {
        /// <summary>
        /// Gets or sets the JWT access token.
        /// </summary>
        public string AccessToken { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the refresh token (if available).
        /// </summary>
        public string? RefreshToken { get; set; }

        /// <summary>
        /// Gets or sets the token expiration time.
        /// </summary>
        public DateTimeOffset ExpiresAt { get; set; }

        /// <summary>
        /// Gets or sets the token type (typically "Bearer").
        /// </summary>
        public string TokenType { get; set; } = "Bearer";

        /// <summary>
        /// Gets or sets the session token (if available).
        /// </summary>
        public string? SessionToken { get; set; }

        /// <summary>
        /// Gets or sets the master token (if available).
        /// </summary>
        public string? MasterToken { get; set; }

        /// <summary>
        /// Gets a value indicating whether the token is expired.
        /// </summary>
        public bool IsExpired => DateTimeOffset.UtcNow >= ExpiresAt;

        /// <summary>
        /// Gets a value indicating whether the token will expire soon (within 5 minutes).
        /// </summary>
        public bool IsExpiringSoon => DateTimeOffset.UtcNow.AddMinutes(5) >= ExpiresAt;

        /// <summary>
        /// Gets a value indicating whether the token can be refreshed.
        /// </summary>
        public bool CanRefresh => !string.IsNullOrEmpty(RefreshToken);
    }
}