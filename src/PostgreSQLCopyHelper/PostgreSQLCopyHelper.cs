using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PostgreSQLCopyHelper;

/// <summary>
/// The delegate that defines how to write a single column for a given entity using the NpgsqlBinaryImporter.
/// </summary>
public delegate ValueTask PgColumnWriter<TEntity>(
    NpgsqlBinaryImporter writer,
    TEntity entity,
    CancellationToken cancellationToken);

/// <summary>
/// A PgType is a simple wrapper around NpgsqlDbType that provides a convenient way to 
/// create column writers for specific types.
/// </summary>
public class PgType<TValue>
{
    public NpgsqlDbType DbType { get; }

    public PgType(NpgsqlDbType dbType)
    {
        DbType = dbType;
    }

    public virtual PgColumnWriter<TEntity> From<TEntity>(Func<TEntity, TValue> extractor)
    {
        return (writer, entity, ct) =>
        {
            TValue value = extractor(entity);

            if (value is null)
            {
                return new ValueTask(writer.WriteNullAsync(ct));
            }

            return new ValueTask(writer.WriteAsync(value, DbType, ct));
        };
    }
}

public static class PgTypeExtensions
{
    public static PgColumnWriter<TEntity> From<TEntity, TValue>(
        this PgType<TValue> pgType,
        Func<TEntity, TValue?> extractor) where TValue : struct
    {
        return async (writer, entity, ct) =>
        {
            TValue? value = extractor(entity);
            if (value.HasValue)
            {
                await writer.WriteAsync(value.Value, pgType.DbType, ct).ConfigureAwait(false);
            }
            else
            {
                await writer.WriteNullAsync(ct).ConfigureAwait(false);
            }
        };
    }

    public static PgType<string> NullCharacterHandling(this PgType<string> pgType, string replacement = "")
    {
        return new SanitizedPgString(pgType.DbType, replacement);
    }
}

internal class SanitizedPgString : PgType<string>
{
    private readonly string _replacement;

    public SanitizedPgString(NpgsqlDbType dbType, string replacement) : base(dbType)
    {
        _replacement = replacement;
    }

    public override PgColumnWriter<TEntity> From<TEntity>(Func<TEntity, string> extractor)
    {
        return (writer, entity, ct) =>
        {
            string value = extractor(entity);

            if (value is null)
            {
                return new ValueTask(writer.WriteNullAsync(ct));
            }

            if (value.Contains('\0'))
            {
                value = value.Replace("\0", _replacement);
            }

            return new ValueTask(writer.WriteAsync(value, DbType, ct));
        };
    }
}

public static class PostgresTypes
{
    // Numeric types
    public static readonly PgType<short> Smallint = new(NpgsqlDbType.Smallint);
    public static readonly PgType<int> Integer = new(NpgsqlDbType.Integer);
    public static readonly PgType<long> Bigint = new(NpgsqlDbType.Bigint);
    public static readonly PgType<float> Real = new(NpgsqlDbType.Real);
    public static readonly PgType<double> DoublePrecision = new(NpgsqlDbType.Double);
    public static readonly PgType<decimal> Numeric = new(NpgsqlDbType.Numeric);
    public static readonly PgType<decimal> Money = new(NpgsqlDbType.Money);

    // Text & String types (Absolutely pure PgType instances)
    public static readonly PgType<string> Text = new(NpgsqlDbType.Text);
    public static readonly PgType<string> Varchar = new(NpgsqlDbType.Varchar);
    public static readonly PgType<string> Char = new(NpgsqlDbType.Char);
    public static readonly PgType<string> Jsonb = new(NpgsqlDbType.Jsonb);
    public static readonly PgType<string> Json = new(NpgsqlDbType.Json);

    // Date & Time
    public static readonly PgType<DateTime> Timestamp = new(NpgsqlDbType.Timestamp);
    public static readonly PgType<DateTime> TimestampTz = new(NpgsqlDbType.TimestampTz);
    public static readonly PgType<DateOnly> Date = new(NpgsqlDbType.Date);
    public static readonly PgType<TimeOnly> Time = new(NpgsqlDbType.Time);

    // Miscellaneous
    public static readonly PgType<bool> Boolean = new(NpgsqlDbType.Boolean);
    public static readonly PgType<byte[]> Bytea = new(NpgsqlDbType.Bytea);
    public static readonly PgType<Guid> Uuid = new(NpgsqlDbType.Uuid);

    // Network types
    public static readonly PgType<IPAddress> Inet = new(NpgsqlDbType.Inet);
    public static readonly PgType<PhysicalAddress> MacAddr = new(NpgsqlDbType.MacAddr);

    // Ranges
    public static readonly PgType<NpgsqlRange<int>> IntegerRange = new(NpgsqlDbType.IntegerRange);
    public static readonly PgType<NpgsqlRange<long>> BigintRange = new(NpgsqlDbType.BigIntRange);
    public static readonly PgType<NpgsqlRange<decimal>> NumericRange = new(NpgsqlDbType.NumericRange);
    public static readonly PgType<NpgsqlRange<DateTime>> TimestampRange = new(NpgsqlDbType.TimestampRange);
    public static readonly PgType<NpgsqlRange<DateTime>> TimestampTzRange = new(NpgsqlDbType.TimestampTzRange);
    public static readonly PgType<NpgsqlRange<DateOnly>> DateRange = new(NpgsqlDbType.DateRange);

    public static PgType<NpgsqlRange<T>> Range<T>(PgType<T> baseType)
    {
        return new PgType<NpgsqlRange<T>>(baseType.DbType | NpgsqlDbType.Range);
    }

    public static PgType<T[]> Array<T>(PgType<T> baseType)
    {
        return new PgType<T[]>(baseType.DbType | NpgsqlDbType.Array);
    }

    public static PgType<List<T>> List<T>(PgType<T> baseType)
    {
        return new PgType<List<T>>(baseType.DbType | NpgsqlDbType.Array);
    }
}

/// <summary>
/// The PgMapper is the central class that defines the mapping between a C# entity and a PostgreSQL table.
/// </summary>
public class PgMapper<TEntity>
{
    private readonly string _tableName;
    private readonly List<string> _columns = new();

    private readonly List<PgColumnWriter<TEntity>> _writers = new();

    public PgMapper(string schemaName, string tableName)
    {
        _tableName = $"\"{schemaName}\".\"{tableName}\"";
    }

    public PgMapper<TEntity> Map(string columnName, PgColumnWriter<TEntity> columnAction)
    {
        _columns.Add($"\"{columnName}\"");
        _writers.Add(columnAction);

        return this;
    }

    public PgMapper<TEntity> Map<TValue>(
        string columnName,
        PgType<TValue> type,
        Func<TEntity, TValue> extractor)
    {
        return Map(columnName, type.From(extractor));
    }

    public PgMapper<TEntity> Map<TValue>(
        string columnName,
        PgType<TValue> type,
        Func<TEntity, TValue?> extractor) where TValue : struct
    {
        return Map(columnName, type.From(extractor));
    }

    internal string GetCopyCommand()
    {
        string columnsSql = string.Join(", ", _columns);

        return $"COPY {_tableName} ({columnsSql}) FROM STDIN BINARY";
    }

    internal IReadOnlyList<PgColumnWriter<TEntity>> GetWriters()
    {
        return _writers;
    }
}

/// <summary>
/// The PgBulkWriter is responsible for executing the bulk insert operation using the NpgsqlBinaryImporter 
/// based on the mapping defined in the PgMapper. It takes care of iterating over the entities and invoking 
/// the appropriate column writers for each entity.
/// </summary>
/// <typeparam name="TEntity"></typeparam>
public class PgBulkWriter<TEntity>
{
    private readonly PgMapper<TEntity> _mapper;

    public PgBulkWriter(PgMapper<TEntity> mapper)
    {
        _mapper = mapper;
    }

    public async Task<ulong> SaveAllAsync(
        NpgsqlConnection connection,
        IEnumerable<TEntity> entities,
        CancellationToken cancellationToken = default)
    {
        string copyCommand = _mapper.GetCopyCommand();
        IReadOnlyList<PgColumnWriter<TEntity>> writers = _mapper.GetWriters();

        await using NpgsqlBinaryImporter importer = await connection
            .BeginBinaryImportAsync(copyCommand, cancellationToken)
            .ConfigureAwait(false);

        foreach (TEntity entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            
            foreach (PgColumnWriter<TEntity> columnWriter in writers)
            {
                await columnWriter(importer, entity, cancellationToken).ConfigureAwait(false);
            }
        }

        return await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }
}