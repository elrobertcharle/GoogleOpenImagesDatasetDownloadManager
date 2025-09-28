using Amazon.S3.Util;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsDatasetDownloadManager
{
    public class Db
    {
        public static string GetConnectionString()
        {
            return @"Host=localhost;Port=5481;Username=admin;Password=pass#4o&6;Database=OpenImages";
        }

        public static async Task Import(string fileName, string table)
        {
            using var sr = File.OpenText(fileName);

            using var conn = new NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync();
            var command = $"INSERT INTO {table}(filename) VALUES(@filename) ON CONFLICT DO NOTHING";
            do
            {
                var line = await sr.ReadLineAsync();
                if (line != null)
                {
                    var s3Filename = line.Split([' '], options: StringSplitOptions.RemoveEmptyEntries)[3];

                    using var cmd = new NpgsqlCommand(command, conn);
                    cmd.Parameters.AddWithValue("filename", s3Filename);
                    await cmd.ExecuteNonQueryAsync();

                }
            } while (!sr.EndOfStream);

        }

        public static async Task ImportFast(string fileName, string table)
        {
            using var sr = File.OpenText(fileName);
            await using var conn = new NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync();

            // Binary COPY into train_files
            await using var writer = await conn.BeginBinaryImportAsync($"COPY {table} (filename, filesize) FROM STDIN (FORMAT BINARY)");

            while (!sr.EndOfStream)
            {
                var line = await sr.ReadLineAsync();
                if (line != null)
                {
                    var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 4)
                    {
                        var fileSize = int.Parse(parts[2]);
                        var s3Filename = parts[3];
                        await writer.StartRowAsync();
                        await writer.WriteAsync(s3Filename, NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(fileSize, NpgsqlTypes.NpgsqlDbType.Integer);
                    }
                }
            }

            await writer.CompleteAsync();
        }

        public static async Task MarkAsDownloaded(string path, string prefix, string table, CancellationToken ct)
        {
            var files = Directory.GetFiles(path);
            if (files.Length == 0)
                return;
            var sb = new StringBuilder($"update {table} set downloaded = true where filename in (");
            for (var i = 0; i < files.Length; i++)
            {
                var separator = i < files.Length - 1 ? "," : "";
                sb.Append($"'{prefix}{Path.GetFileName(files[i])}'{separator}");
            }
            sb.Append(")");
            var command = sb.ToString();
            using var conn = new NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync(ct);
            using var cmd = new NpgsqlCommand(sb.ToString(), conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public static async Task<List<string>> LeftoverFiles(string path, string prefix, string table, CancellationToken ct)
        {
            var files = Directory.GetFiles(path);
            using var conn = new NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync(ct);

            var fileNames = files.Select(f => prefix + Path.GetFileName(f)).ToList();

            using var sqlCommand = new NpgsqlCommand(@$"
            SELECT unnest(@filenames) AS fname
            EXCEPT
            SELECT filename FROM {table};", conn);

            sqlCommand.Parameters.AddWithValue("filenames", fileNames);

            using var reader = await sqlCommand.ExecuteReaderAsync(ct);
            var result = new List<string>();
            while (await reader.ReadAsync(ct))
            {
                var f = reader.GetString(0);
                result.Add(f);
                Console.WriteLine("Missing: " + f);
            }

            return result;
        }
    }
}
