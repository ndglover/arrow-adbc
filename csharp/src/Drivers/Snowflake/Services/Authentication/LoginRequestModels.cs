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

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.Authentication;

/// <summary>
/// Represents the login request body sent to Snowflake.
/// </summary>
internal class LoginRequestBody
{
    /// <summary>
    /// Gets or sets the login request data.
    /// </summary>
    [JsonPropertyName("data")]
    public LoginRequestData Data { get; set; } = new();
}

/// <summary>
/// Represents the data portion of the login request.
/// </summary>
internal class LoginRequestData
{
    /// <summary>
    /// Gets or sets the client application ID.
    /// </summary>
    [JsonPropertyName("CLIENT_APP_ID")]
    public string CLIENT_APP_ID { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the client application version.
    /// </summary>
    [JsonPropertyName("CLIENT_APP_VERSION")]
    public string CLIENT_APP_VERSION { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the Snowflake account name.
    /// </summary>
    [JsonPropertyName("ACCOUNT_NAME")]
    public string ACCOUNT_NAME { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the login name (username).
    /// </summary>
    [JsonPropertyName("LOGIN_NAME")]
    public string LOGIN_NAME { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the password.
    /// </summary>
    [JsonPropertyName("PASSWORD")]
    public string PASSWORD { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the authenticator type.
    /// </summary>
    [JsonPropertyName("AUTHENTICATOR")]
    public string AUTHENTICATOR { get; set; } = "snowflake";

    /// <summary>
    /// Gets or sets the client environment information.
    /// </summary>
    [JsonPropertyName("CLIENT_ENVIRONMENT")]
    public ClientEnvironment CLIENT_ENVIRONMENT { get; set; } = new();

    /// <summary>
    /// Gets or sets the session parameters.
    /// </summary>
    [JsonPropertyName("SESSION_PARAMETERS")]
    public Dictionary<string, object> SESSION_PARAMETERS { get; set; } = new();
}
