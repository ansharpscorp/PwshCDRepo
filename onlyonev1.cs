// Solution: TeamsCDRDownloader
// .NET 8 Console App - Full Project Structure
// Dependencies: Newtonsoft.Json, Microsoft.Extensions.Configuration, Polly

// Directory Structure:
// /TeamsCDRDownloader
// ├── appsettings.json
// ├── Program.cs
// ├── Downloader
// │     └── CallRecordDownloader.cs
// ├── Helpers
// │     ├── ConfigLoader.cs
// │     ├── GraphHelper.cs
// │     └── Logger.cs
// ├── Models
// │     └── CallRecord.cs
// └── TeamsCDRDownloader.csproj

/* appsettings.json */
{
  "GraphAPI": {
    "ClientId": "<client-id>",
    "TenantId": "<tenant-id>",
    "ClientSecret": "<client-secret>",
    "Scope": "https://graph.microsoft.com/.default"
  },
  "Paths": {
    "CsvInputPath": "./Input",
    "JsonOutputPath": "./Output",
    "FailedCsvPath": "./Failed",
    "LogPath": "./Logs/app.log"
  }
}

/* Program.cs */
using Microsoft.Extensions.Configuration;
using TeamsCDRDownloader.Helpers;
using TeamsCDRDownloader.Downloader;

var config = ConfigLoader.LoadConfig();
var graphHelper = new GraphHelper(config);
var downloader = new CallRecordDownloader(config, graphHelper);

string date = args[0];
string csvFile = args[1];

await downloader.ProcessConferenceIdsAsync(date, csvFile);

/* Helpers/ConfigLoader.cs */
using Microsoft.Extensions.Configuration;

namespace TeamsCDRDownloader.Helpers
{
    public static class ConfigLoader
    {
        public static IConfiguration LoadConfig() => new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();
    }
}

/* Helpers/Logger.cs */
namespace TeamsCDRDownloader.Helpers
{
    public class Logger
    {
        private readonly string LogPath;
        private readonly object _lock = new object();

        public Logger(string logPath)
        {
            LogPath = logPath ?? throw new ArgumentNullException(nameof(logPath));
        }

        public void Log(string message)
        {
            lock (_lock)
            {
                File.AppendAllText(LogPath, $"{DateTime.Now}: {message}\n");
            }
        }
    }
}

/* Helpers/GraphHelper.cs */
using Microsoft.Identity.Client;
using Microsoft.Extensions.Configuration;

namespace TeamsCDRDownloader.Helpers
{
    public class GraphHelper
    {
        private string AccessToken = string.Empty;
        private DateTime ExpiryTime;
        private readonly string ClientId, TenantId, ClientSecret, Scope;

        public GraphHelper(IConfiguration config)
        {
            var section = config.GetSection("GraphAPI");
            ClientId = section["ClientId"] ?? throw new ArgumentNullException("ClientId");
            TenantId = section["TenantId"] ?? throw new ArgumentNullException("TenantId");
            ClientSecret = section["ClientSecret"] ?? throw new ArgumentNullException("ClientSecret");
            Scope = section["Scope"] ?? throw new ArgumentNullException("Scope");
            ExpiryTime = DateTime.UtcNow.AddMinutes(-5);
        }

        public async Task<string> GetAccessTokenAsync()
        {
            if (DateTime.UtcNow >= ExpiryTime)
            {
                var app = ConfidentialClientApplicationBuilder
                    .Create(ClientId)
                    .WithTenantId(TenantId)
                    .WithClientSecret(ClientSecret)
                    .Build();

                var result = await app.AcquireTokenForClient(new[] { Scope }).ExecuteAsync();
                AccessToken = result.AccessToken;
                ExpiryTime = DateTime.UtcNow.AddMinutes(50);
            }
            return AccessToken;
        }
    }
}

/* Downloader/CallRecordDownloader.cs */
using Newtonsoft.Json.Linq;
using Polly;
using TeamsCDRDownloader.Helpers;

namespace TeamsCDRDownloader.Downloader
{
    public class CallRecordDownloader
    {
        private readonly IConfiguration _config;
        private readonly GraphHelper _graphHelper;
        private readonly Logger _logger;
        private readonly string CsvInputPath, JsonOutputPath, FailedCsvPath;

        public CallRecordDownloader(IConfiguration config, GraphHelper graphHelper)
        {
            _config = config;
            _graphHelper = graphHelper;
            _logger = new Logger(config["Paths:LogPath"]);
            CsvInputPath = config["Paths:CsvInputPath"] ?? "./Input";
            JsonOutputPath = config["Paths:JsonOutputPath"] ?? "./Output";
            FailedCsvPath = config["Paths:FailedCsvPath"] ?? "./Failed";
        }

        public async Task ProcessConferenceIdsAsync(string date, string csvFile)
        {
            var lines = File.ReadAllLines(Path.Combine(CsvInputPath, csvFile)).Skip(1);
            var tasks = lines.Select(id => ProcessCallRecordAsync(id, date));
            await Task.WhenAll(tasks);
        }

        private async Task ProcessCallRecordAsync(string confId, string date)
        {
            try
            {
                string token = await _graphHelper.GetAccessTokenAsync();
                var client = new HttpClient();

                // Sessions + Segments
                var sessionUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}/sessions?$expand=segments";
                var sessionData = await FetchAllPagesAsync(client, sessionUrl, token);

                // Participants_v2
                var participantUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}/participants_v2";
                var participantData = await FetchAllPagesAsync(client, participantUrl, token);

                // Base Call Record
                var baseUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}";
                var baseContent = await GetContentAsync(client, baseUrl, token);
                JObject baseJson = JObject.Parse(baseContent);

                // Build Output
                var output = new JObject
                {
                    ["id"] = baseJson["id"],
                    ["type"] = baseJson["type"],
                    ["modalities"] = baseJson["modalities"],
                    ["version"] = baseJson["version"],
                    ["joinWebUrl"] = baseJson["joinWebUrl"],
                    ["participants"] = baseJson["participants"],
                    ["organizer"] = baseJson["organizer"],
                    ["organizer_v2"] = baseJson["organizer_v2"],
                    ["participants_v2"] = participantData,
                    ["sessions"] = sessionData
                };

                string outDir = Path.Combine(JsonOutputPath, DateTime.Parse(date).ToString("yyyy/MMM/dd"));
                Directory.CreateDirectory(outDir);
                File.WriteAllText(Path.Combine(outDir, confId + ".json"), output.ToString());
            }
            catch (Exception ex)
            {
                _logger.Log($"Error processing {confId}: {ex.Message}");
                File.AppendAllText(Path.Combine(FailedCsvPath, "Failed.csv"), confId + "\n");
            }
        }

        private async Task<JArray> FetchAllPagesAsync(HttpClient client, string url, string token)
        {
            var data = new JArray();
            while (!string.IsNullOrEmpty(url))
            {
                var content = await GetContentAsync(client, url, token);
                JObject root = JObject.Parse(content);
                if (root["value"] != null)
                    data.Merge(root["value"]);

                url = root["@odata.nextLink"]?.ToString();
            }
            return data;
        }

        private async Task<string> GetContentAsync(HttpClient client, string url, string token)
        {
            return await Policy
                .Handle<HttpRequestException>()
                .Or<TaskCanceledException>()
                .WaitAndRetryAsync(3, retry => TimeSpan.FromSeconds(2))
                .ExecuteAsync(async () =>
                {
                    var request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
                    var response = await client.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadAsStringAsync();
                });
        }
    }
}

/* Models/CallRecord.cs */
// Optional for strongly typed JSON if needed

/* TeamsCDRDownloader.csproj */
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.Configuration.Json" Version="8.0.0" />
    <PackageReference Include="Microsoft.Identity.Client" Version="4.55.0" />
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageReference Include="Polly" Version="7.2.3" />
  </ItemGroup>
</Project>

// Run Example:
// dotnet run -- 2025-06-01 ConfId_20250601.csv
