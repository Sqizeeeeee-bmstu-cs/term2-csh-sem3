using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using CsvModel;
using System.Collections.Generic;


// TODO: part 1: sqlite

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

// -------------------------------------------------------------------------------------------

// TODO: part 2: raw csv







static CsvTable ReadCsv(TextReader reader, char separator)
{
    string headerLine = reader.ReadLine();
    if (headerLine is null)
        throw new InvalidOperationException("Входной поток пуст — нет строки заголовков.");

    string[] headers = headerLine.Split(separator);
    var rows = new List<CsvRow>();

    string line;
    while ((line = reader.ReadLine()) is not null)
    {
        if (string.IsNullOrWhiteSpace(line))
            continue;

        string[] parts = line.Split(separator);
        rows.Add(new CsvRow(parts));
    }

    return new CsvTable(headers, rows);
}

static void WriteCsv(TextWriter writer, CsvTable table, char separator)
{
    writer.WriteLine(string.Join(separator, table.Headers));

    foreach (var row in table.Rows)
        writer.WriteLine(string.Join(separator, row.Fields));
}

static int FindColumnIndex(CsvTable table, string columnName)
{
    int index = Array.IndexOf(table.Headers, columnName);
    if (index < 0)
        throw new ArgumentException(
            $"Колонка «{columnName}» не найдена. " +
            $"Доступные колонки: {string.Join(", ", table.Headers)}");
    return index;
}

static CsvTable ProjectionCsv(CsvTable table, string columnName)
{
    int colIndex = FindColumnIndex(table, columnName);
    string[] newHeaders = [columnName];
    var newRows = new List<CsvRow>();

    foreach (var row in table.Rows)
    {
        string[] fields = [row.Fields[colIndex]];
        newRows.Add(new CsvRow(fields));
    }

    return new CsvTable(newHeaders, newRows);
}

static CsvTable WhereCsv(CsvTable table, string columnName, string value)
{
    int colIndex = FindColumnIndex(table, columnName);
    var newRows = new List<CsvRow>();

    foreach (var row in table.Rows)
    {
        if (row.Fields[colIndex] == value)
        {
            newRows.Add(row);
        }
    }

    return new CsvTable(table.Headers, newRows);
}

static CsvTable JoinCsv(CsvTable left, CsvTable right, string leftKey, string rightKey)
{
    int leftKeyIndex = FindColumnIndex(left, leftKey);
    int rightKeyIndex = FindColumnIndex(right, rightKey);

    // Заголовки: все колонки левой + все колонки правой
    var newHeaders = new string[left.Headers.Length + right.Headers.Length];
    for (int i = 0; i < left.Headers.Length; i++)
        newHeaders[i] = left.Headers[i];
    for (int i = 0; i < right.Headers.Length; i++)
        newHeaders[left.Headers.Length + i] = right.Headers[i];

    var newRows = new List<CsvRow>();

    foreach (var leftRow in left.Rows)
    {
        foreach (var rightRow in right.Rows)
        {
            if (leftRow.Fields[leftKeyIndex] == rightRow.Fields[rightKeyIndex])
            {
                // Склеиваем поля двух строк
                var fields = new string[leftRow.Fields.Length + rightRow.Fields.Length];
                for (int i = 0; i < leftRow.Fields.Length; i++)
                    fields[i] = leftRow.Fields[i];
                for (int i = 0; i < rightRow.Fields.Length; i++)
                    fields[leftRow.Fields.Length + i] = rightRow.Fields[i];
                
                newRows.Add(new CsvRow(fields));
            }
        }
    }

    return new CsvTable(newHeaders, newRows);
}

static double AverageCsv(List<double> values)
{
    double sum = 0;
    for (int i = 0; i < values.Count; i++)
        sum += values[i];
    return sum / values.Count;
}

static CsvTable GroupAvgCsv(CsvTable table, string groupColumn, string valueColumn)
{
    int groupIndex = FindColumnIndex(table, groupColumn);
    int valueIndex = FindColumnIndex(table, valueColumn);

    // Ключ - значение колонки группировки, значение - список чисел в группе
    var groups = new Dictionary<string, List<double>>();

    foreach (var row in table.Rows)
    {
        string key = row.Fields[groupIndex];
        double value = double.Parse(row.Fields[valueIndex]);
        
        if (!groups.ContainsKey(key))
            groups[key] = new List<double>();
        groups[key].Add(value);
    }

    // Формируем результат
    string[] newHeaders = [groupColumn, "avg_" + valueColumn];
    var newRows = new List<CsvRow>();

    foreach (var pair in groups)
    {
        string avg = AverageCsv(pair.Value).ToString("F2");
        newRows.Add(new CsvRow([pair.Key, avg]));
    }

    return new CsvTable(newHeaders, newRows);
}

// --------------------------------------------------------------------------------------------

string dbPath = "developers.db";

if (args.Length > 0)
{
    // Режим работы с CSV через аргументы командной строки
    string mode = args[0].ToLower();
    
    switch (mode)
    {
        case "projection":
            // CSV поступает из стандартного ввода
            var table = ReadCsv(Console.In, ';');
            string columnName = args[1];
            var result = ProjectionCsv(table, columnName);
            WriteCsv(Console.Out, result, ';');
            break;
            
        case "where":
        {
            if (args.Length < 3)
            {
                Console.Error.WriteLine("Использование: program where <колонка> <значение>");
                return;
            }
            var tableWhere = ReadCsv(Console.In, ';');
            string colName = args[1];
            string value = args[2];
            var resultWhere = WhereCsv(tableWhere, colName, value);
            WriteCsv(Console.Out, resultWhere, ';');
            break;
        }
            
        case "join":
        {
            if (args.Length < 5)
            {
                Console.Error.WriteLine("Использование: program join <файл1> <файл2> <ключ1> <ключ2>");
                return;
            }
            using var reader1 = File.OpenText(args[1] + ".csv");
            using var reader2 = File.OpenText(args[2] + ".csv");
            var left = ReadCsv(reader1, ';');
            var right = ReadCsv(reader2, ';');
            var resultJoin = JoinCsv(left, right, args[3], args[4]);
            WriteCsv(Console.Out, resultJoin, ';');
            break;
        }    

        case "group_avg":
            if (args.Length < 3)
            {
                Console.Error.WriteLine("Использование: program group_avg <колонка_группировки> <колонка_значений>");
                return;
            }
            var tableGroup = ReadCsv(Console.In, ';');
            string groupColumn = args[1];
            string valueColumn = args[2];
            var resultGroup = GroupAvgCsv(tableGroup, groupColumn, valueColumn);
            WriteCsv(Console.Out, resultGroup, ';');
            break;
            
        default:
            Console.WriteLine("Неизвестный режим. Доступные режимы: projection, where, join, group_avg");
            break;
    }
}
else
{
    // Режим работы с SQLite (как раньше)
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
}




// dotnet run projection dev_name < dev.csv
// dotnet run where dep_id 2 < dev.csv
// dotnet run group_avg dep_id dev_commits < dev.csv
// dotnet run (sqlite mode)
