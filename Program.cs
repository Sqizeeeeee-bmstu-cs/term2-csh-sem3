// See https://aka.ms/new-console-template for more information

using System.Data;
using Microsoft.Data.Sqlite;


static void CreateDatabase(string dbPath)
{
    if (File.Exists(dbPath))
    {
        File.Delete(dbPath);
    }
    
    using var connection = new SqliteConnection($"Data Source={dbPath}");
    
    connection.Open();

    var command = connection.CreateCommand();
    command.CommandText = "CREATE TABLE dep (dep_id INTEGER PRIMARY KEY, dep_name TEXT NOT NULL)";
    command.ExecuteNonQuery();


    command.CommandText = "CREATE TABLE dev (dev_id INTEGER PRIMARY KEY, dep_id INTEGER NOT NULL, dev_name TEXT NOT NULL, dev_commits INTEGER NOT NULL, FOREIGN KEY (dep_id) REFERENCES dep(dep_id))";
    command.ExecuteNonQuery();


    Console.WriteLine("Файл создан");
}

static void PrintData(string dbPath, string tableName)
{
    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM {tableName} ORDER BY 1;";

    var reader = cmd.ExecuteReader();

    int columnCount = reader.FieldCount;

    Console.WriteLine("========== Table ==========");

    for (int i = 0; i < columnCount; i++)
    {
        Console.Write($"{reader.GetName(i),-20}");
    }
    Console.WriteLine();
    Console.WriteLine(new string('-', 20 * columnCount));
    
    // Выводим строки
    while (reader.Read())
    {
        for (int i = 0; i < columnCount; i++)
        {
            Console.Write($"{reader.GetValue(i),-20}");
        }
        Console.WriteLine();
    }
}

static void LoadData(string dbPath, string devCsvPath, string depCsvPath)
{
    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();
    

    using (var transaction = connection.BeginTransaction())
    {
        var lines = File.ReadAllLines(depCsvPath);

        for (int i = 1; i < lines.Length; i++)
        {
            var parts = lines[i].Split(';');
            var cmd = connection.CreateCommand();
            cmd.CommandText = "INSERT INTO dep (dep_id, dep_name) VALUES (@id, @name);";
            cmd.Parameters.AddWithValue("@id", int.Parse(parts[0]));
            cmd.Parameters.AddWithValue("@name", parts[1]);
            cmd.ExecuteNonQuery();
        }
        transaction.Commit();
        Console.WriteLine($"[OK] Загружено строк из {depCsvPath}: {lines.Length - 1}");
    }
    
    using (var transaction = connection.BeginTransaction())
    {
        var lines = File.ReadAllLines(devCsvPath);
        for (int i = 1; i < lines.Length; i++)
        {
            var parts = lines[i].Split(';');
            if (parts.Length < 4) continue;
            
            var cmd = connection.CreateCommand();
            cmd.CommandText = "INSERT INTO dev (dev_id, dep_id, dev_name, dev_commits) VALUES (@devId, @depId, @name, @commits);";
            
            cmd.Parameters.AddWithValue("@devId", int.Parse(parts[0]));
            cmd.Parameters.AddWithValue("@depId", int.Parse(parts[1]));
            cmd.Parameters.AddWithValue("@name", parts[2]);
            cmd.Parameters.AddWithValue("@commits", int.Parse(parts[3]));
            
            cmd.ExecuteNonQuery();
        }
        transaction.Commit();
        Console.WriteLine($"[OK] Загружено строк из {devCsvPath}: {lines.Length - 1}");
    }
}

static List<string> Projection(string dbPath, string tableName, string columnName)
{
    var result = new List<string>();

    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT {columnName} FROM {tableName} ORDER BY 1;";

    using var reader = cmd.ExecuteReader();

    while (reader.Read())
    {
        result.Add(reader.GetValue(0).ToString());
    }
    
    return result;
}

static List<string> Where(string dbPath, string tableName, string columnName, string value)
{
    var result = new List<string>();
    
    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM {tableName} WHERE {columnName} = @val ORDER BY 1;";
    cmd.Parameters.AddWithValue("@val", value);

    using var reader = cmd.ExecuteReader();

    while (reader.Read())
    {
        var row = "";
        for (int c = 0; c < reader.FieldCount; c++)
        {
            if (c > 0)
            {
                row += " | ";
            }

            row += reader.GetValue(c).ToString();
        }
        result.Add(row);
    }
    
    return result;
}

static (string[] columns, List<string[]> rows) Join(
    string dbPath, string table1, string table2, string key1, string key2)
{
    var rows = new List<string[]>();

    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM {table1} INNER JOIN {table2} ON {table1}.{key1} = {table2}.{key2} ORDER BY 1;";

    using var reader = cmd.ExecuteReader();

    var columns = new string[reader.FieldCount];
    for (int i = 0; i < reader.FieldCount; i++)
        columns[i] = reader.GetName(i);

    while (reader.Read())
    {
        var row = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
            row[i] = reader.GetValue(i).ToString();
        rows.Add(row);
    }
    
    return (columns, rows);
}

static (string[] columns, List<string[]> rows) GroupAvg(
    string dbPath, string tableName, string groupColumn, string avgColumn)
{
    var rows = new List<string[]>();

    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT {groupColumn}, AVG({avgColumn}) AS avg_{avgColumn} FROM {tableName} GROUP BY {groupColumn} ORDER BY 1;";

    using var reader = cmd.ExecuteReader();

    var columns = new string[reader.FieldCount];
    for (int i = 0; i < reader.FieldCount; i++)
        columns[i] = reader.GetName(i);

    while (reader.Read())
    {
        var row = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
            row[i] = reader.GetValue(i).ToString();
        rows.Add(row);
    }
    
    return (columns, rows);
}



string dbPath = "developers.db";
CreateDatabase(dbPath);
Console.WriteLine("\n=== Просмотр БД ===");
LoadData(dbPath, "dev.csv", "dep.csv");
PrintData(dbPath, "dep");
PrintData(dbPath, "dev");

var names = Projection(dbPath, "dev", "dev_name");
Console.WriteLine("\n=== Имена разработчиков ===");
foreach (var name in names)
    Console.WriteLine(name);


var developersFromDep2 = Where(dbPath, "dev", "dep_id", "2");
Console.WriteLine("\n=== Разработчики из отдела 2 ===");
foreach (var dev in developersFromDep2)
    Console.WriteLine(dev);

var (joinColumns, joinRows) = Join(dbPath, "dev", "dep", "dep_id", "dep_id");
Console.WriteLine("\n=== Результат Join(dev, dep, dep_id, dep_id) ===");
Console.WriteLine(string.Join(" | ", joinColumns));
Console.WriteLine(new string('-', 80));
foreach (var row in joinRows)
    Console.WriteLine(string.Join(" | ", row));


var (avgCols, avgRows) = GroupAvg(dbPath, "dev", "dep_id", "dev_commits");
Console.WriteLine("\n=== Среднее количество коммитов по отделам ===");
Console.WriteLine(string.Join(" | ", avgCols));
Console.WriteLine(new string('-', 40));
foreach (var row in avgRows)
    Console.WriteLine(string.Join(" | ", row));

