using Amazon.Runtime;
using Amazon.S3;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsDatasetDownloadManager
{
    public class Downloader
    {
        private const string bucketName = "open-images-dataset";
        private static readonly AmazonS3Client s3Client = new AmazonS3Client(new AnonymousAWSCredentials(), new AmazonS3Config
        {
            ForcePathStyle = true,
            RegionEndpoint = Amazon.RegionEndpoint.USEast1
        });

        public static async Task<bool> DownloadBatch(int batchSize, string localFolder, NpgsqlConnection connection, CancellationToken ct)
        {
            bool endOfDb = false;

            // Step 1: Get the next N files not yet downloaded
            var files = new List<(int id, string filename)>();
            using (var cmd = new NpgsqlCommand("SELECT id, filename FROM train_files WHERE downloaded = false LIMIT @limit", connection))
            {
                cmd.Parameters.AddWithValue("limit", batchSize);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    files.Add((reader.GetInt32(0), reader.GetString(1)));
                }
            }

            if (files.Count < batchSize)
            {
                endOfDb = true;
            }

            var sw = new Stopwatch();

            // Step 2: Download each file
            foreach (var (id, filename) in files)
            {
                sw.Start();
                string localPath = Path.Combine(localFolder, Path.GetFileName(filename));
                ct.ThrowIfCancellationRequested();

                var response = await s3Client.GetObjectAsync(bucketName, filename, ct);
                if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
                    throw new Exception($"HttpStatusCode: " + response.HttpStatusCode);

                using var responseStream = response.ResponseStream;
                // await using var fileStream = File.Create(localPath);

                await using var fileStream = new FileStream(localPath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, useAsync: true);

                await responseStream.CopyToAsync(fileStream, ct);

                // Step 3: Mark as downloaded
                using var updateCmd = new NpgsqlCommand("UPDATE train_files SET downloaded = true WHERE id = @id", connection);
                updateCmd.Parameters.AddWithValue("id", id);
                await updateCmd.ExecuteNonQueryAsync(ct);

                //Console.WriteLine($"File with id={id} was successfully downloaded.");

                sw.Stop();
                Console.WriteLine($"Time per file: {sw.ElapsedMilliseconds}ms");
            }
            return endOfDb;
        }
    }
}
