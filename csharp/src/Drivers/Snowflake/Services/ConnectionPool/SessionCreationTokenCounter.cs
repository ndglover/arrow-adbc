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
using System.Linq;
using System.Threading;

namespace Apache.Arrow.Adbc.Drivers.Snowflake.Services.ConnectionPool;

internal class SessionCreationTokenCounter
{
    private readonly TimeSpan _timeout;
    private readonly ReaderWriterLockSlim _tokenLock = new();
    private readonly List<SessionCreationToken> _tokens = new();

    public SessionCreationTokenCounter(TimeSpan timeout)
    {
        _timeout = timeout;
    }

    public SessionCreationToken NewToken()
    {
        _tokenLock.EnterWriteLock();
        try
        {
            var token = new SessionCreationToken(_timeout);
            _tokens.Add(token);
            var now = DateTimeOffset.UtcNow;
            _tokens.RemoveAll(t => t.IsExpired(now));
            return token;
        }
        finally
        {
            _tokenLock.ExitWriteLock();
        }
    }

    public void RemoveToken(SessionCreationToken creationToken)
    {
        _tokenLock.EnterWriteLock();
        try
        {
            var now = DateTimeOffset.UtcNow;
            _tokens.RemoveAll(t => creationToken.Id == t.Id || t.IsExpired(now));
        }
        finally
        {
            _tokenLock.ExitWriteLock();
        }
    }

    public int Count()
    {
        _tokenLock.EnterReadLock();
        try
        {
            return _tokens.Count;
        }
        finally
        {
            _tokenLock.ExitReadLock();
        }
    }

    public void Reset()
    {
        _tokenLock.EnterWriteLock();
        try
        {
            _tokens.Clear();
        }
        finally
        {
            _tokenLock.ExitWriteLock();
        }
    }
}
