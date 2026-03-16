using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using NpgsqlTypes;

namespace PostgreSQLCopyHelper.Test;

[TestClass]
public class PgBulkWriterIntegrationTest
{
    private const string ConnectionString = "Host=localhost;Port=5431;Database=postgres;Username=postgres;Password=password;";

    private NpgsqlConnection? _connection;

    [TestInitialize]
    public async Task Setup()
    {
        _connection = new NpgsqlConnection(ConnectionString);

        await _connection.OpenAsync();

        // Ensure a clean slate before every test run
        using NpgsqlCommand cmd = _connection.CreateCommand();
        cmd.CommandText = @"
                DROP TABLE IF EXISTS integration_test_data;
                CREATE TABLE integration_test_data (
                    id int8 PRIMARY KEY,
                    text_val text,
                    numeric_val numeric,
                    is_active boolean,
                    created_at timestamp,
                    date_val date,
                    time_val time,
                    timestamptz_val timestamptz,
                    int_range int4range,
                    ts_range tsrange,
                    tags text[],
                    smallint_val int2,
                    integer_val int4,
                    real_val float4,
                    double_val float8,
                    money_val money,
                    varchar_val varchar(50),
                    char_val char(10),
                    json_val json,
                    jsonb_val jsonb,
                    bytea_val bytea,
                    uuid_val uuid,
                    inet_val inet,
                    macaddr_val macaddr
                );";

        await cmd.ExecuteNonQueryAsync();
    }

    [TestCleanup]
    public async Task Cleanup()
    {
        if (_connection != null)
        {
            // Clean up the table after the test is done
            using NpgsqlCommand cmd = _connection.CreateCommand();

            cmd.CommandText = "DROP TABLE IF EXISTS integration_test_data;";

            await cmd.ExecuteNonQueryAsync();

            await _connection.DisposeAsync();
        }
    }

    // Record for holding test data across various PostgreSQL types
    public record TestEntity(
        long Id,
        string? TextVal,
        decimal NumericVal,
        bool IsActive,
        DateTime CreatedAt,
        DateOnly DateVal,
        TimeOnly TimeVal,
        DateTime TimestampTzVal,
        NpgsqlRange<int> IntRange,
        NpgsqlRange<DateTime> TsRange,
        string[]? Tags,
        short SmallintVal,
        int IntegerVal,
        float RealVal,
        double DoubleVal,
        decimal MoneyVal,
        string VarcharVal,
        string CharVal,
        string JsonVal,
        string JsonbVal,
        byte[] ByteaVal,
        Guid? UuidVal, 
        IPAddress InetVal,
        PhysicalAddress MacAddrVal
    );

    [TestMethod]
    public async Task BulkInsert_SavesDataCorrectly_IncludingExtensiveTypes()
    {
        // Configure the mapper with all necessary columns and types
        var mapper = new PgMapper<TestEntity>("public", "integration_test_data")
            .Map("id", PostgresTypes.Bigint, x => x.Id)
            .Map("text_val", PostgresTypes.Text.NullCharacterHandling(""), x => x.TextVal)
            .Map("numeric_val", PostgresTypes.Numeric, x => x.NumericVal)
            .Map("is_active", PostgresTypes.Boolean, x => x.IsActive)
            .Map("created_at", PostgresTypes.Timestamp, x => x.CreatedAt)
            .Map("date_val", PostgresTypes.Date, x => x.DateVal)
            .Map("time_val", PostgresTypes.Time, x => x.TimeVal)
            .Map("timestamptz_val", PostgresTypes.TimestampTz, x => x.TimestampTzVal)
            .Map("int_range", PostgresTypes.IntegerRange, x => x.IntRange)
            .Map("ts_range", PostgresTypes.TimestampRange, x => x.TsRange)
            .Map("tags", PostgresTypes.Array(PostgresTypes.Text), x => x.Tags)
            .Map("smallint_val", PostgresTypes.Smallint, x => x.SmallintVal)
            .Map("integer_val", PostgresTypes.Integer, x => x.IntegerVal)
            .Map("real_val", PostgresTypes.Real, x => x.RealVal)
            .Map("double_val", PostgresTypes.DoublePrecision, x => x.DoubleVal)
            .Map("money_val", PostgresTypes.Money, x => x.MoneyVal)
            .Map("varchar_val", PostgresTypes.Varchar, x => x.VarcharVal)
            .Map("char_val", PostgresTypes.Char, x => x.CharVal)
            .Map("json_val", PostgresTypes.Json, x => x.JsonVal)
            .Map("jsonb_val", PostgresTypes.Jsonb, x => x.JsonbVal)
            .Map("bytea_val", PostgresTypes.Bytea, x => x.ByteaVal)
            .Map("uuid_val", PostgresTypes.Uuid, x => x.UuidVal)
            .Map("inet_val", PostgresTypes.Inet, x => x.InetVal)
            .Map("macaddr_val", PostgresTypes.MacAddr, x => x.MacAddrVal);

        // Create the bulk writer with the configured mapper
        var writer = new PgBulkWriter<TestEntity>(mapper);

        // Generate some Test Data
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var nowUtc = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var today = new DateOnly(2026, 1, 1);
        var timeNow = new TimeOnly(12, 0, 0);
        var guid1 = Guid.NewGuid();

        var entities = new List<TestEntity>
            {
                new TestEntity(
                    Id: 1L,
                    TextVal: "Normal text",
                    NumericVal: 42.1234m,
                    IsActive: true,
                    CreatedAt: now,
                    DateVal: today,
                    TimeVal: timeNow,
                    TimestampTzVal: nowUtc,
                    IntRange: new NpgsqlRange<int>(1, true, 100, false),
                    TsRange: new NpgsqlRange<DateTime>(now.AddDays(-1), true, now, true),
                    Tags: new[] { "csharp", "postgres" },
                    
                    SmallintVal: (short)42,
                    IntegerVal: 123456,
                    RealVal: 3.14f,
                    DoubleVal: 2.718281828,
                    MoneyVal: 99.99m,
                    VarcharVal: "Hello Varchar",
                    CharVal: "ABC       ",
                    JsonVal: "{\"key\":\"value\"}",
                    JsonbVal: "{\"active\": true}",
                    ByteaVal: new byte[] { 0x01, 0x02, 0x03 },
                    UuidVal: guid1,
                    InetVal: IPAddress.Parse("192.168.1.100"), // IPv4
                    MacAddrVal: PhysicalAddress.Parse("001422012345")
                ),
                new TestEntity(
                    Id: 2L,
                    TextVal: "Evil \u0000 Text",
                    NumericVal: -99.99m,
                    IsActive: false,
                    CreatedAt: now.AddDays(-1),
                    DateVal: today.AddDays(-1),
                    TimeVal: timeNow.AddHours(-1),
                    TimestampTzVal: nowUtc.AddDays(-1),
                    IntRange: NpgsqlRange<int>.Empty,
                    TsRange: new NpgsqlRange<DateTime>(now, true, false, default, false, true),
                    Tags: new[] { "test", null!, "array" },
                    
                    SmallintVal: (short)-1,
                    IntegerVal: -987654,
                    RealVal: -0.99f,
                    DoubleVal: -123.456,
                    MoneyVal: -10.50m,
                    VarcharVal: "Another text",
                    CharVal: "X         ",
                    JsonVal: "[1, 2, 3]",
                    JsonbVal: "{\"nested\": {\"a\": 1}}",
                    ByteaVal: Array.Empty<byte>(),
                    UuidVal: null,
                    InetVal: IPAddress.Parse("2001:db8::ff00:42:8329"), // IPv6
                    MacAddrVal: PhysicalAddress.Parse("08002B010203")
                )
            };

        // Execute the Bulk Insert
        Assert.IsNotNull(_connection);

        ulong inserted = await writer.SaveAllAsync(_connection, entities);
        
        Assert.AreEqual(2ul, inserted);

        // Verify Data from DB
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM integration_test_data ORDER BY id";
        using var reader = await cmd.ExecuteReaderAsync();

        // Check Row 1
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(1L, reader.GetInt64(0));
        Assert.AreEqual("Normal text", reader.GetString(1));
        Assert.AreEqual((short) 42, reader.GetInt16(11));
        Assert.AreEqual(123456, reader.GetInt32(12));
        Assert.AreEqual(3.14f, reader.GetFloat(13));
        Assert.AreEqual(2.718281828, reader.GetDouble(14));
        Assert.AreEqual(99.99m, reader.GetDecimal(15));
        Assert.AreEqual("Hello Varchar", reader.GetString(16));
        Assert.AreEqual("ABC       ", reader.GetString(17));

        Assert.IsTrue(reader.GetString(18).Contains("\"key\"")); // JSON
        Assert.IsTrue(reader.GetString(19).Contains("\"active\"")); // JSONB

        var bytea1 = reader.GetFieldValue<byte[]>(20);
        Assert.AreEqual(3, bytea1.Length);
        Assert.AreEqual((byte) 0x02, bytea1[1]);

        Assert.AreEqual(guid1, reader.GetGuid(21));
        Assert.AreEqual(IPAddress.Parse("192.168.1.100"), reader.GetFieldValue<IPAddress>(22));
        Assert.AreEqual(PhysicalAddress.Parse("001422012345"), reader.GetFieldValue<PhysicalAddress>(23));

        // Check Row 2
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(2L, reader.GetInt64(0));
        Assert.AreEqual("Evil  Text", reader.GetString(1)); // Null character removed
        Assert.AreEqual((short) -1, reader.GetInt16(11));
        Assert.AreEqual(-987654, reader.GetInt32(12));
        Assert.AreEqual(-0.99f, reader.GetFloat(13));
        Assert.AreEqual(-123.456, reader.GetDouble(14));
        Assert.AreEqual(-10.50m, reader.GetDecimal(15));
        Assert.AreEqual("Another text", reader.GetString(16));
        Assert.AreEqual("X         ", reader.GetString(17));

        var bytea2 = reader.GetFieldValue<byte[]>(20);
        Assert.AreEqual(0, bytea2.Length); // Empty array works

        Assert.IsTrue(reader.IsDBNull(21)); // UuidVal was successfully mapped as NULL
        Assert.AreEqual(IPAddress.Parse("2001:db8::ff00:42:8329"), reader.GetFieldValue<IPAddress>(22));
        Assert.AreEqual(PhysicalAddress.Parse("08002B010203"), reader.GetFieldValue<PhysicalAddress>(23));

        // Ensure no extra rows
        Assert.IsFalse(await reader.ReadAsync());
    }
}
