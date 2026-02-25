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
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Snowflake.Configuration;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication
{
    /// <summary>
    /// Provides authentication services for Snowflake connections.
    /// </summary>
    public class AuthenticationService : IAuthenticationService
    {
        private readonly IBasicAuthenticator _basicAuth;
        private readonly IKeyPairAuthenticator _keyPairAuth;
        private readonly IOAuthAuthenticator _oauthAuth;
        private readonly ISsoAuthenticator _ssoAuth;
        private readonly ConcurrentDictionary<string, AuthenticationToken> _tokenCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="AuthenticationService"/> class.
        /// </summary>
        /// <param name="basicAuth">The basic authenticator.</param>
        /// <param name="keyPairAuth">The key pair authenticator.</param>
        /// <param name="oauthAuth">The OAuth authenticator.</param>
        /// <param name="ssoAuth">The SSO authenticator.</param>
        public AuthenticationService(
            IBasicAuthenticator basicAuth,
            IKeyPairAuthenticator keyPairAuth,
            IOAuthAuthenticator oauthAuth,
            ISsoAuthenticator ssoAuth)
        {
            _basicAuth = basicAuth ?? throw new ArgumentNullException(nameof(basicAuth));
            _keyPairAuth = keyPairAuth ?? throw new ArgumentNullException(nameof(keyPairAuth));
            _oauthAuth = oauthAuth ?? throw new ArgumentNullException(nameof(oauthAuth));
            _ssoAuth = ssoAuth ?? throw new ArgumentNullException(nameof(ssoAuth));
            _tokenCache = new ConcurrentDictionary<string, AuthenticationToken>();
        }

        /// <inheritdoc/>
        public async Task<AuthenticationToken> AuthenticateAsync(
            string account,
            string user,
            AuthenticationConfig config,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(account))
                throw new ArgumentException("Account cannot be null or empty.", nameof(account));
            
            if (string.IsNullOrEmpty(user))
                throw new ArgumentException("User cannot be null or empty.", nameof(user));

            if (config == null)
                throw new ArgumentNullException(nameof(config));

            // Validate configuration
            var validationResults = config.Validate();
            if (validationResults.Any())
            {
                var errors = string.Join(", ", validationResults.Select(r => r.ErrorMessage));
                throw new ArgumentException($"Invalid authentication configuration: {errors}", nameof(config));
            }

            // Route to appropriate authenticator based on type
            return config.Type switch
            {
                AuthenticationType.UsernamePassword => await _basicAuth.AuthenticateAsync(account, user, config.Password!, cancellationToken),
                AuthenticationType.KeyPair => await _keyPairAuth.AuthenticateAsync(account, user, config.PrivateKeyPath!, config.PrivateKeyPassphrase, cancellationToken),
                AuthenticationType.OAuth => await _oauthAuth.AuthenticateAsync(account, user, config.OAuthToken!, cancellationToken),
                AuthenticationType.Sso or AuthenticationType.ExternalBrowser => await _ssoAuth.AuthenticateAsync(account, user, config.SsoProperties, cancellationToken),
                _ => throw new NotSupportedException($"Authentication type {config.Type} is not supported.")
            };
        }

        /// <inheritdoc/>
        public async Task<AuthenticationToken> RefreshTokenAsync(
            AuthenticationToken token,
            CancellationToken cancellationToken = default)
        {
            if (token == null)
                throw new ArgumentNullException(nameof(token));

            if (!token.CanRefresh)
                throw new InvalidOperationException("Token cannot be refreshed. No refresh token available.");

            return await _oauthAuth.RefreshTokenAsync(token.RefreshToken!, cancellationToken);
        }

        /// <inheritdoc/>
        public void InvalidateToken(AuthenticationToken token)
        {
            if (token == null)
                throw new ArgumentNullException(nameof(token));

            // Remove from cache if present
            var cacheKey = token.AccessToken;
            _tokenCache.TryRemove(cacheKey, out _);
        }


    }
}
