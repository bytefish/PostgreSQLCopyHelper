using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PostgreSQLCopyHelper.Test;

[TestClass]
public class PgBulkWriterFirewallTest
{
    /// <summary>
    /// Docker PostgreSQL connection string for testing. It assumes a PostgreSQL instance is running locally on 
    /// port 5431 with the default "postgres" database and credentials.
    /// </summary>
    private const string ConnectionString = "Host=localhost;Port=5431;Database=postgres;Username=postgres;Password=password;";

    private NpgsqlConnection? _connection;

    /// <summary>
    /// Creates the necessary database schema and table for testing the bulk insert of firewall rules, ensuring 
    /// a clean state before each test runs. It establishes a connection to the PostgreSQL database and executes 
    /// SQL commands to set up the "network.firewall_rules" table.
    /// </summary>
    [TestInitialize]
    public async Task Setup()
    {
        _connection = new NpgsqlConnection(ConnectionString);
        await _connection.OpenAsync();

        // Setup the specific network schema and table for this test
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
                DROP TABLE IF EXISTS network.firewall_rules;
                DROP SCHEMA IF EXISTS network CASCADE;
                CREATE SCHEMA network;
                
                CREATE TABLE network.firewall_rules (
                    rule_id uuid PRIMARY KEY,
                    server_name text,
                    vlan_id int4,
                    open_ports int4range[]
                );";
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Cleans up the database by dropping the "network.firewall_rules" table and the "network" schema after each test.
    /// </summary>
    [TestCleanup]
    public async Task Cleanup()
    {
        if (_connection != null)
        {
            using var cmd = _connection.CreateCommand();

            cmd.CommandText = "DROP TABLE IF EXISTS network.firewall_rules; DROP SCHEMA IF EXISTS network CASCADE;";

            await cmd.ExecuteNonQueryAsync();

            await _connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Domain Model representing a firewall rule for a server, which includes a unique identifier, the server 
    /// name, an optional VLAN assignment, and a list of open port ranges. This model is designed to be mapped 
    /// to a PostgreSQL table using the PgMapper, allowing for efficient bulk insertion of firewall rules 
    /// into the database.
    /// </summary>
    public record FirewallRule(
        Guid RuleId,
        string ServerName,
        int? AssignedVlan,
        List<NpgsqlRange<int>> OpenPorts
    );

    /// <summary>
    /// Maps the FirewallRule domain model to the corresponding PostgreSQL table structure. It 
    /// defines how each property of the FirewallRule record corresponds to a column in the 
    /// "network.firewall_rules" table.
    /// </summary>
    private static readonly PgMapper<FirewallRule> FirewallMapper =
        new PgMapper<FirewallRule>("network", "firewall_rules")
            .Map("rule_id", PostgresTypes.Uuid, x => x.RuleId)
            .Map("server_name", PostgresTypes.Text, x => x.ServerName)
            .Map("vlan_id", PostgresTypes.Integer, x => x.AssignedVlan)
            .Map("open_ports", PostgresTypes.List(PostgresTypes.IntegerRange), x => x.OpenPorts);

    /// <summary>
    /// Checks the bulk insertion of firewall rules with port ranges into the PostgreSQL database.
    /// </summary>
    [TestMethod]
    public async Task BulkInsert_FirewallRulesWithRanges_SavesCorrectly()
    {
        var ruleId = Guid.NewGuid();

        var rulesToInsert = new List<FirewallRule>
            {
                new FirewallRule(
                    RuleId: ruleId,
                    ServerName: "web-prod-01",
                    AssignedVlan: 101,
                    OpenPorts: new List<NpgsqlRange<int>>
                    { 
                        // Port 80 (Inclusive bound representation)
                        new NpgsqlRange<int>(80, true, 80, true),
                        // Port 443 (Inclusive bound representation)
                        new NpgsqlRange<int>(443, true, 443, true)
                    }
                )
            };

        // The PgBulkWriter is responsible for inserting large collections of FirewallRule records
        // into the PostgreSQL database.
        var writer = new PgBulkWriter<FirewallRule>(FirewallMapper);

        // Execute Bulk Insert
        Assert.IsNotNull(_connection);
        ulong insertedCount = await writer.SaveAllAsync(_connection, rulesToInsert);
        Assert.AreEqual(1ul, insertedCount);

        // Verify Data from DB
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM network.firewall_rules";
        using var reader = await cmd.ExecuteReaderAsync();

        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(ruleId, reader.GetGuid(0));
        Assert.AreEqual("web-prod-01", reader.GetString(1));
        Assert.AreEqual(101, reader.GetInt32(2));

        // Read back the array of ranges
        var retrievedRanges = reader.GetFieldValue<NpgsqlRange<int>[]>(3);
        Assert.AreEqual(2, retrievedRanges.Length);

        // Verification of the actual range logic
        Assert.AreEqual(80, retrievedRanges[0].LowerBound);
        Assert.AreEqual(443, retrievedRanges[1].LowerBound);

        // Ensure no extra rows
        Assert.IsFalse(await reader.ReadAsync());
    }
}