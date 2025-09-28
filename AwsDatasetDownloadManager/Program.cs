using AwsDatasetDownloadManager;
using Microsoft.Extensions.Logging;
using Npgsql;



var cts = new CancellationTokenSource();

var localPath = @"E:\Datasets\OpenImages\validation";
var table = "validation_files";
var prefix = "validation/";

//await Db.LeftoverFiles(localPath, prefix, table, cts.Token);

//await Db.ImportFast(@"C:\Users\elrob\validation_files.txt", table);
//return;



// Cancel when Ctrl+C is pressed
Console.CancelKeyPress += (sender, eventArgs) =>
{
    Console.WriteLine("Cancellation requested...");
    eventArgs.Cancel = true; // prevent process from terminating immediately
    cts.Cancel();
};


await Db.MarkAsDownloaded(localPath, prefix, table, cts.Token);

try
{
    await using var conn = new NpgsqlConnection(Db.GetConnectionString());
    await conn.OpenAsync();

    while (!cts.IsCancellationRequested)
    {
        var endOfDb = await Downloader.DownloadBatchParallel(10, table, localPath, conn, cts.Token);
        if (endOfDb)
            return;
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Download canceled by user.");
}