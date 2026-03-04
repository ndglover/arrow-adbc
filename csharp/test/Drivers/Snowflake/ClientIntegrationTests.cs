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
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake;

/// <summary>
/// Integration tests that execute real queries against Snowflake.
/// </summary>
public class ClientIntegrationTests
{
    private const string SampleDataTable = "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES";

    private readonly ITestOutputHelper _output;
    private readonly SnowflakeTestConfiguration _testConfiguration;

    public ClientIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = SnowflakeTestingUtils.TestConfiguration;

        Skip.If(
            string.IsNullOrWhiteSpace(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE}`");
    }

    [SkippableTheory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(1000000)]
    public async Task CanExecuteSampleDataQueryAsync(int limit)
    {
        var driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);

        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();

        statement.SqlQuery = CreateSampleDataQuery(limit);
        var stopwatch = Stopwatch.StartNew();

        var result = await statement.ExecuteQueryAsync();

        Assert.NotNull(result);
        Assert.NotNull(result.Stream);

        using var stream = result.Stream;
        var schema = stream.Schema;
        long totalRows = 0;
        int batchCount = 0;
        RecordBatch? batch;

        Assert.NotNull(schema);
        Assert.True(schema.FieldsList.Count > 0);

        while ((batch = await stream.ReadNextRecordBatchAsync()) != null)
        {
            using (batch)
            {
                batchCount++;
                Assert.Equal(schema.FieldsList.Count, batch.ColumnCount);
                Assert.True(batch.Length > 0);
                totalRows += batch.Length;
            }
        }

        Assert.True(batchCount > 0);
        Assert.True(totalRows <= limit);
        Assert.Equal(totalRows, result.RowCount);
        Assert.Equal(limit, totalRows);
        stopwatch.Stop();

        _output.WriteLine($"Fetched {totalRows} rows in {batchCount} batches with {schema.FieldsList.Count} columns for limit {limit} in {stopwatch.ElapsedMilliseconds} ms.");
    }

    private static string CreateSampleDataQuery(int limit) =>
        $"SELECT * FROM {SampleDataTable} LIMIT {limit}";
}
