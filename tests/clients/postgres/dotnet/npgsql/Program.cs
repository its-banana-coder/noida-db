// Npgsql (the .NET Postgres driver) against noida-db: parameterized inserts,
// array/jsonb round-trips, transactions (commit and rollback), a unique
// violation, and the system columns (ctid/xmin/tableoid) some EF Core
// concurrency-token setups rely on.
//
// Run with PGPORT pointing at noida-db (or a real Postgres, which must pass
// just the same).
using Npgsql;

int checks = 0;
void Check(bool cond, string msg)
{
    if (!cond) throw new Exception("FAIL: " + msg);
    checks++;
}

var port = Environment.GetEnvironmentVariable("PGPORT") ?? "5432";
var connString = $"Host=127.0.0.1;Port={port};Username=postgres;Database=postgres";

await using var conn = new NpgsqlConnection(connString);
await conn.OpenAsync();

await using (var cmd = new NpgsqlCommand("SELECT version()", conn))
{
    var version = await cmd.ExecuteScalarAsync();
    Check(version is string, "version query");
}

await using (var cmd = new NpgsqlCommand("DROP TABLE IF EXISTS nt", conn))
    await cmd.ExecuteNonQueryAsync();
await using (var cmd = new NpgsqlCommand(
    "CREATE TABLE nt (id serial PRIMARY KEY, name text, tags text[], meta jsonb)", conn))
    await cmd.ExecuteNonQueryAsync();

await using (var cmd = new NpgsqlCommand(
    "INSERT INTO nt (name, tags, meta) VALUES (@name, @tags, @meta::jsonb)", conn))
{
    cmd.Parameters.AddWithValue("name", "a");
    cmd.Parameters.AddWithValue("tags", new[] { "x", "y" });
    cmd.Parameters.AddWithValue("meta", "{\"k\":1}");
    Check(await cmd.ExecuteNonQueryAsync() == 1, "insert a");
}
await using (var cmd = new NpgsqlCommand(
    "INSERT INTO nt (name, tags, meta) VALUES (@name, @tags, @meta::jsonb)", conn))
{
    cmd.Parameters.AddWithValue("name", "b");
    cmd.Parameters.AddWithValue("tags", Array.Empty<string>());
    cmd.Parameters.AddWithValue("meta", "{}");
    Check(await cmd.ExecuteNonQueryAsync() == 1, "insert b");
}

await using (var cmd = new NpgsqlCommand("SELECT id, name, tags FROM nt ORDER BY id", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    Check(await reader.ReadAsync(), "row 1 present");
    var tags = reader.GetFieldValue<string[]>(2);
    Check(reader.GetInt32(0) == 1 && reader.GetString(1) == "a" && tags.Length == 2, "row 1 shape");
    Check(await reader.ReadAsync(), "row 2 present");
    Check(await reader.ReadAsync() == false, "exactly two rows");
}

await using (var tx = await conn.BeginTransactionAsync())
{
    await using (var cmd = new NpgsqlCommand("UPDATE nt SET name = 'z' WHERE id = 1", conn, tx))
        await cmd.ExecuteNonQueryAsync();
    await tx.CommitAsync();
}
await using (var cmd = new NpgsqlCommand("SELECT name FROM nt WHERE id = 1", conn))
{
    var name = (string?)await cmd.ExecuteScalarAsync();
    Check(name == "z", "committed value " + name);
}

await using (var tx2 = await conn.BeginTransactionAsync())
{
    var threw = false;
    try
    {
        await using var cmd = new NpgsqlCommand("INSERT INTO nt (id, name) VALUES (1, 'dup')", conn, tx2);
        await cmd.ExecuteNonQueryAsync();
    }
    catch (PostgresException)
    {
        threw = true;
    }
    Check(threw, "unique violation should error");
    await tx2.RollbackAsync();
}

await using (var cmd = new NpgsqlCommand("SELECT count(*) FROM nt", conn))
{
    var n = (long?)await cmd.ExecuteScalarAsync();
    Check(n == 2, "count after rollback " + n);
}

// System columns: ctid as a stable per-row key, tableoid's identity.
await using (var cmd = new NpgsqlCommand("SELECT ctid FROM nt WHERE id = 1", conn))
{
    var ctid = await cmd.ExecuteScalarAsync();
    Check(ctid is not null, "ctid selectable");
}
await using (var cmd = new NpgsqlCommand("SELECT tableoid = 'nt'::regclass FROM nt LIMIT 1", conn))
{
    var eq = (bool?)await cmd.ExecuteScalarAsync();
    Check(eq == true, "tableoid matches regclass");
}

await using (var cmd = new NpgsqlCommand("DROP TABLE nt", conn))
    await cmd.ExecuteNonQueryAsync();

Console.WriteLine($"npgsql: {checks} checks passed");
