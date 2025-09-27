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

        public static async Task Import(string fileName)
        {
            using var sr = File.OpenText(fileName);

            using var conn = new NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync();
            var command = "INSERT INTO train_files(filename) VALUES(@filename) ON CONFLICT DO NOTHING";
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
    }
}
