using AwsDatasetDownloadManager;
using Npgsql;

var cts = new CancellationTokenSource();

// Cancel when Ctrl+C is pressed
Console.CancelKeyPress += (sender, eventArgs) =>
{
    Console.WriteLine("Cancellation requested...");
    eventArgs.Cancel = true; // prevent process from terminating immediately
    cts.Cancel();
};


try
{
    await using var conn = new NpgsqlConnection(Db.GetConnectionString());
    await conn.OpenAsync();

    while (!cts.IsCancellationRequested)
    {
        var endOfDb = await Downloader.DownloadBatch(10, @"F:\Datasets\OpenImages\train", conn, cts.Token);
        if (endOfDb)
            return;
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Download canceled by user.");
}