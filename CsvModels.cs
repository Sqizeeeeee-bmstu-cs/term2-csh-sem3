namespace CsvModel;

public record CsvRow(string[] Fields);

public record CsvTable(string[] Headers, List<CsvRow> Rows);
