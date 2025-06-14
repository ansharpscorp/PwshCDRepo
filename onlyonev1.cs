TeamsCDRDownloader/
│
├── Program.cs
├── Downloader
│   ├── CallRecordDownloader.cs
│   ├── GraphHelper.cs
│   ├── Logger.cs
│   └── ConfigLoader.cs
├── Models
│   └── AppConfig.cs
├── appsettings.json
├── TeamsCDRDownloader.csproj
└── README.md

// Program.cs
using Downloader;
using Models;

class Program
{
    static async Task Main(string[] args)
    {
        var config = ConfigLoader.Load();
        var inputCsvPath = args[1];
        var startDate = args[0];
        await CallRecordDownloader.ProcessConferenceIdsAsync(config, startDate, inputCsvPath);
    }
}

// Downloader/ConfigLoader.cs
using Microsoft.Extensions.Configuration;
using Models;

namespace Downloader
{
    public static class ConfigLoader
    {
        public static AppConfig Load()
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
            return config.Get<AppConfig>();
        }
    }
}

// Models/AppConfig.cs
namespace Models
{
    public class AppConfig
    {
        public string TenantId { get; set; }
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string GraphScope { get; set; }
        public string CsvFolder { get; set; }
        public string OutputFolder { get; set; }
        public string LogFile { get; set; }
        public int MaxRetry { get; set; }
        public int MaxParallel { get; set; }
    }
}

// Downloader/GraphHelper.cs
using Microsoft.Identity.Client;
using Models;

namespace Downloader
{
    public static class GraphHelper
    {
        private static IConfidentialClientApplication app;
        private static AuthenticationResult result;
        private static DateTime expiry;
        private static readonly object tokenLock = new object();

        public static string GetAccessToken(AppConfig config)
        {
            lock (tokenLock)
            {
                if (result == null || expiry < DateTime.UtcNow)
                {
                    app = ConfidentialClientApplicationBuilder
                        .Create(config.ClientId)
                        .WithClientSecret(config.ClientSecret)
                        .WithTenantId(config.TenantId)
                        .Build();

                    result = app.AcquireTokenForClient(new[] { config.GraphScope }).ExecuteAsync().Result;
                    expiry = DateTime.UtcNow.AddMinutes(50);
                }
                return result.AccessToken;
            }
        }
    }
}

// Downloader/Logger.cs
namespace Downloader
{
    public static class Logger
    {
        private static readonly object logLock = new object();
        public static void Log(string message, string logPath)
        {
            lock (logLock)
            {
                File.AppendAllText(logPath, $"{DateTime.Now}: {message}\n");
            }
        }
    }
}

// Downloader/CallRecordDownloader.cs
using Newtonsoft.Json.Linq;
using Polly;
using CsvHelper;
using System.Globalization;
using Models;

namespace Downloader
{
    public static class CallRecordDownloader
    {
        public static async Task ProcessConferenceIdsAsync(AppConfig config, string date, string inputCsv)
        {
            var ids = File.ReadAllLines(Path.Combine(config.CsvFolder, inputCsv)).Skip(1);
            Directory.CreateDirectory(config.OutputFolder);

            var policy = Policy.Handle<Exception>()
                .WaitAndRetryAsync(config.MaxRetry, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

            await Parallel.ForEachAsync(ids, new ParallelOptions { MaxDegreeOfParallelism = config.MaxParallel }, async (id, ct) =>
            {
                await policy.ExecuteAsync(async () =>
                {
                    string token = GraphHelper.GetAccessToken(config);
                    var client = new HttpClient();

                    // Main CallRecord
                    var url = $"https://graph.microsoft.com/v1.0/communications/callRecords/{id}";
                    var mainResp = await client.GetStringAsync(url);
                    JObject mainObj = JObject.Parse(mainResp);

                    // participants_v2 Pagination
                    var participantsUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{id}/participants_v2";
                    var participants = await FetchPagedDataAsync(client, participantsUrl, token);

                    // sessions Pagination
                    var sessionsUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{id}/sessions?$expand=segments";
                    var sessions = await FetchPagedDataAsync(client, sessionsUrl, token);

                    // Build final JSON
                    JObject output = new JObject
                    {
                        ["id"] = mainObj["id"],
                        ["type"] = mainObj["type"],
                        ["modalities"] = mainObj["modalities"],
                        ["version"] = mainObj["version"],
                        ["joinWebUrl"] = mainObj["joinWebUrl"],
                        ["participants"] = mainObj["participants"],
                        ["organizer"] = mainObj["organizer"],
                        ["organizer_v2"] = mainObj["organizer_v2"],
                        ["participants_v2"] = participants,
                        ["sessions"] = sessions
                    };

                    string outPath = Path.Combine(config.OutputFolder, date, DateTime.Parse(date).ToString("yyyy/MMM/dd"));
                    Directory.CreateDirectory(outPath);
                    File.WriteAllText(Path.Combine(outPath, $"{id}.json"), output.ToString());
                });
            });
        }

        private static async Task<JArray> FetchPagedDataAsync(HttpClient client, string url, string token)
        {
            var result = new JArray();
            while (!string.IsNullOrEmpty(url))
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Add("Authorization", $"Bearer {token}");
                var response = await client.SendAsync(request);
                string content = await response.Content.ReadAsStringAsync();
                var json = JObject.Parse(content);
                if (json["value"] != null)
                    result.Merge(json["value"]);
                url = json["@odata.nextLink"]?.ToString();
            }
            return result;
        }
    }
}

// appsettings.json
{
  "TenantId": "<tenant>",
  "ClientId": "<client>",
  "ClientSecret": "<secret>",
  "GraphScope": "https://graph.microsoft.com/.default",
  "CsvFolder": "./CSV",
  "OutputFolder": "./Output",
  "LogFile": "./log.txt",
  "MaxRetry": 3,
  "MaxParallel": 4
}

// TeamsCDRDownloader.csproj
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Identity.Client" Version="4.54.0" />
    <PackageReference Include="Microsoft.Extensions.Configuration.Json" Version="6.0.0" />
    <PackageReference Include="CsvHelper" Version="30.0.1" />
    <PackageReference Include="Polly" Version="7.2.3" />
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
  </ItemGroup>
</Project>
