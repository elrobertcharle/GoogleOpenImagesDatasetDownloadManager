// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

await AwsDatasetDownloadManager.Db.MarkAsDownloaded(@"F:\Datasets\OpenImages\train", "train/", "train_files");