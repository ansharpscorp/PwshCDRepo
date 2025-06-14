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
using System;
using System.Threading.Tasks;

namespace Downloader
{
    public static class GraphHelper
    {
        private static string? _accessToken = null;
        private static DateTime _expiry = DateTime.MinValue;
        private static readonly object _lock = new();

        public static async Task<string> GetAccessTokenAsync()
        {
            lock (_lock)
            {
                if (!string.IsNullOrEmpty(_accessToken) && _expiry > DateTime.UtcNow.AddMinutes(5))
                {
                    return _accessToken;
                }
            }

            var app = ConfidentialClientApplicationBuilder
                .Create(ConfigLoader.Config["GraphAPI:ClientId"])
                .WithClientSecret(ConfigLoader.Config["GraphAPI:ClientSecret"])
                .WithTenantId(ConfigLoader.Config["GraphAPI:TenantId"])
                .Build();

            var scopes = new[] { ConfigLoader.Config["GraphAPI:Scope"] };

            var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();

            lock (_lock)
            {
                _accessToken = result.AccessToken;
                _expiry = DateTime.UtcNow.AddSeconds(result.ExpiresIn);
            }

            return _accessToken!;
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
using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Downloader;

namespace Downloader
{
    public class CallRecordDownloader
    {
        private static readonly HttpClient _httpClient = new HttpClient();

        public static async Task<JObject?> DownloadCallRecordAsync(string conferenceId)
        {
            string token = await GraphHelper.GetAccessTokenAsync();
            string baseUrl = $"{ConfigLoader.Config["GraphAPI:BaseUrl"]}/communications/callRecords/{conferenceId}";

            // Main CallRecord data
            JObject root = await GetApiDataAsync($"{baseUrl}", token);

            if (root == null)
            {
                Logger.LogError($"Failed to fetch CallRecord for {conferenceId}");
                return null;
            }

            // participants_v2 (paginated)
            var participantsV2 = await GetPaginatedDataAsync($"{baseUrl}/participants_v2", token);
            root["participants_v2"] = participantsV2 != null ? JArray.FromObject(participantsV2) : new JArray();

            // sessions expanded by segments (paginated)
            var sessions = await GetPaginatedDataAsync($"{baseUrl}/sessions?$expand=segments", token);
            root["sessions"] = sessions != null ? JArray.FromObject(sessions) : new JArray();

            return root;
        }

        private static async Task<JObject?> GetApiDataAsync(string url, string token)
        {
            return await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(2 * retryAttempt))
                .ExecuteAsync(async () =>
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

                    using var response = await _httpClient.SendAsync(request);
                    response.EnsureSuccessStatusCode();

                    var content = await response.Content.ReadAsStringAsync();
                    return JObject.Parse(content);
                });
        }

        private static async Task<JArray?> GetPaginatedDataAsync(string url, string token)
        {
            var allItems = new JArray();

            while (!string.IsNullOrEmpty(url))
            {
                var page = await GetApiDataAsync(url, token);

                if (page?["value"] != null)
                {
                    allItems.Merge(page["value"]);
                }

                url = page?["@odata.nextLink"]?.ToString();
            }

            return allItems;
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
