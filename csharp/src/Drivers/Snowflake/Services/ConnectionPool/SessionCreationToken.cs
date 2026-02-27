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

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

internal class SessionCreationToken
{
    public Guid Id { get; }
    private readonly DateTimeOffset _grantedAt;
    private readonly TimeSpan _timeout;

    public SessionCreationToken(TimeSpan timeout)
    {
        Id = Guid.NewGuid();
        _grantedAt = DateTimeOffset.UtcNow;
        _timeout = timeout;
    }

    public bool IsExpired(DateTimeOffset now) =>
        now - _grantedAt > _timeout;
}
