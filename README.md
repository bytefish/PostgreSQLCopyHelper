# PostgreSQLCopyHelper #

PostgreSQLCopyHelper is a high-performance .NET library for Bulk Inserts to PostgreSQL using the Binary COPY Protocol.

It provides an elegant, highly optimized wrapper around the PostgreSQL COPY command:

> The COPY command is a PostgreSQL specific feature, which allows efficient bulk import or export of
> data to and from a table. This is a much faster way of getting data in and out of a table than using
> `INSERT` and `SELECT`.

This project wouldn't be possible without the great Npgsql library, which handles the underlying Postgres wire protocol.

## Setup ##

PostgreSQLCopyHelper is available on NuGet.

You can add the following dependency to your .csproj to include it in your project:

```xml
<PackageReference Include="PostgreSQLCopyHelper" Version="3.0.0" />
```

## PostgreSQLCopyHelper 3.0.0 ###

The newest major release of PostgreSQLCopyHelper comes with a completely redesigned API.

The new API strictly separates the **What** (Structure and Mapping) from the **How** (Execution and I/O). It flips 
the mental model by having a Database-first API, which solves a lot of problem with the previous API and allows for 
composing complex types more easily. 

## Quick Start ##

### 1. Define your Data Model ###

The library works perfectly with modern C# record types, structs, or traditional classes.

```csharp
public record UserSession(
    Guid Id,
    string? UserAgent,   // Nullable Reference Type
    DateTime CreatedAt,  // Precise Timestamp
    int[] Tags,          // Array
    NpgsqlRange<int> ActiveRange // Native Range Support
);
```

### 2. Define your Mapping (Stateless & Thread-Safe) ###

The `PgMapper<T>` is the heart of the library. It is completely stateless after configuration and should be 
instantiated only once (e.g., as a `static readonly` field or Singleton).

```csharp
private static readonly PgMapper<UserSession> SessionMapper = 
    new PgMapper<UserSession>("public", "user_sessions")
        .Map("id", PostgresTypes.Uuid, x => x.Id)
                
        // SAFE STRINGS: Strips invalid \u0000 characters to prevent pipeline crashes
        .Map("user_agent", PostgresTypes.Text.NullCharacterHandling(""), x => x.UserAgent)
        
        // TIME TYPES: Native support for Npgsql's DateTime semantics
        .Map("created_at", PostgresTypes.TimestampTz, x => x.CreatedAt)
        
        // ARRAYS: Compose base types natively
        .Map("tags", PostgresTypes.Array(PostgresTypes.Integer), x => x.Tags)

        // RANGES: Native Postgres range types
        .Map("active_range", PostgresTypes.IntegerRange, x => x.ActiveRange);
```

### 3. Execute the Bulk Insert ###

The `PgBulkWriter<T>` is a lightweight, transient executor that takes your mapper and streams the data to the 
database using `ValueTask` and asynchronous I/O.

```csharp
public async Task SaveSessionsAsync(NpgsqlConnection conn, List<UserSession> sessionList)
{
    var writer = new PgBulkWriter<UserSession>(SessionMapper);
    
    ulong insertedCount = await writer.SaveAllAsync(conn, sessionList);
    Console.WriteLine($"Successfully inserted {insertedCount} sessions.");
}
```

## Streaming and Lazy Evaluation ##

One of the key strengths of the `SaveAllAsync` method is that it accepts an `IEnumerable<T>`. This means you 
are never forced to load your entire dataset into memory.

If you are yielding data from a stream, a file parser, or another database, the writer will pull the data lazily:

```csharp
IEnumerable<UserSession> massiveDataStream = ReadMassiveDataFromCsv();

// Data is streamed directly to PostgreSQL on-the-fly. Memory consumption remains flat.
await writer.SaveAllAsync(connection, massiveDataStream);
```

## Mastering the Fluent API ##

The API is designed around `PostgresTypes`. This class serves as your single entry point for all PostgreSQL data types.

When you map a property, the compiler automatically detects if your `struct` is nullable (`int?`) or non-nullable (`int`):

```csharp
// The compiler routes this to the high-performance, non-allocating path
.Map("mandatory_id", PostgresTypes.Integer, x => x.Id) // Id is 'int'

// The compiler routes this to the null-safe path automatically!
.Map("optional_bonus", PostgresTypes.Integer, x => x.Bonus) // Bonus is 'int?'
```

## Advanced Type Mapping ##

### Arrays and Lists ###

You can compose any base type into an array or list using the Array() or List() composition functions:

```csharp
// Maps a C# List<string> to a Postgres text[]
.Map("nicknames", PostgresTypes.List(PostgresTypes.Text), x => x.Nicknames)

// Maps a C# int[] to a Postgres int4[]
.Map("scores", PostgresTypes.Array(PostgresTypes.Integer), x => x.Scores)
```

### Ranges ###

PostgreSQL's powerful range types are fully supported via `NpgsqlRange<T>`:

```csharp
// Using predefined common ranges
.Map("age_limit", PostgresTypes.IntegerRange, x => x.AgeRange)

// Composing custom ranges dynamically (e.g. for custom PostgreSql Range Types)
.Map("custom_range", PostgresTypes.Range(PostgresTypes.DoublePrecision), x => x.CustomRange)
```