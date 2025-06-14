TeamsCDRDownloader/
│
├── Program.cs
├── Downloader
│   ├── CallRecordDownloader.cs
│   ├── ConferenceIdReader.cs
│   └── GraphHelper.cs
├── Helpers
│   ├── ConfigLoader.cs
│   └── Logger.cs
├── appsettings.json
├── TeamsCDRDownloader.csproj
└── README.md

--- Program.cs ---

using System;
using System.Threading.Tasks;
using TeamsCDRDownloader.Downloader;
using TeamsCDRDownloader.Helpers;

namespace TeamsCDRDownloader
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: TeamsCDRDownloader <Date> <ConferenceIdCsvFilePath>");
                return;
            }

            string date = args[0];
            string csvPath = args[1];

            Logger.Init();
            Logger.LogInfo($"Starting download for date: {date}");

            var callRecords = ConferenceIdReader.ReadConferenceIds(csvPath);
            var downloader = new CallRecordDownloader();
            await downloader.ProcessCallRecordsAsync(callRecords, date);

            Logger.LogInfo("Download process completed.");
        }
    }
}

--- Downloader/ConferenceIdReader.cs ---

using System.Collections.Generic;
using System.IO;

namespace TeamsCDRDownloader.Downloader
{
    public static class ConferenceIdReader
    {
        public static List<string> ReadConferenceIds(string csvPath)
        {
            var ids = new List<string>();
            foreach (var line in File.ReadLines(csvPath))
            {
                if (!line.Contains("ConferenceId"))
                    ids.Add(line.Trim());
            }
            return ids;
        }
    }
}

--- Downloader/GraphHelper.cs ---

using Microsoft.Identity.Client;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using TeamsCDRDownloader.Helpers;

namespace TeamsCDRDownloader.Downloader
{
    public static class GraphHelper
    {
        private static string accessToken;
        private static DateTimeOffset expiry;

        public static async Task<string> GetAccessTokenAsync()
        {
            if (string.IsNullOrEmpty(accessToken) || DateTimeOffset.UtcNow >= expiry)
            {
                var app = ConfidentialClientApplicationBuilder.Create(ConfigLoader.Config["GraphAPI:ClientId"])
                    .WithClientSecret(ConfigLoader.Config["GraphAPI:ClientSecret"])
                    .WithTenantId(ConfigLoader.Config["GraphAPI:TenantId"])
                    .Build();

                var result = await app.AcquireTokenForClient(new[] { ConfigLoader.Config["GraphAPI:Scope"] }).ExecuteAsync();
                accessToken = result.AccessToken;
                expiry = result.ExpiresOn;
            }
            return accessToken;
        }

        public static async Task<string> GetGraphDataAsync(string url)
        {
            using var httpClient = new HttpClient();
            var token = await GetAccessTokenAsync();
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var response = await httpClient.GetAsync(url);
            return await response.Content.ReadAsStringAsync();
        }
    }
}

--- Downloader/CallRecordDownloader.cs ---

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using TeamsCDRDownloader.Helpers;

namespace TeamsCDRDownloader.Downloader
{
    public class CallRecordDownloader
    {
        public async Task ProcessCallRecordsAsync(List<string> conferenceIds, string date)
        {
            foreach (var id in conferenceIds)
            {
                try
                {
                    string baseUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{id}";
                    string mainData = await GraphHelper.GetGraphDataAsync(baseUrl);

                    var root = JObject.Parse(mainData);
                    root["participants_v2"] = await GetExpandedDataAsync($"{baseUrl}/participants_v2");
                    root["sessions"] = await GetExpandedDataAsync($"{baseUrl}/sessions?$expand=segments");

                    string outputDir = Path.Combine("Output", DateTime.Parse(date).ToString("yyyy/MMM/dd"));
                    Directory.CreateDirectory(outputDir);
                    string filePath = Path.Combine(outputDir, $"{id}.json");
                    await File.WriteAllTextAsync(filePath, root.ToString());
                }
                catch (Exception ex)
                {
                    Logger.LogError($"Error processing {id}: {ex.Message}");
                }
            }
        }

        private async Task<JArray> GetExpandedDataAsync(string url)
        {
            var data = new JArray();
            do
            {
                var result = await GraphHelper.GetGraphDataAsync(url);
                var root = JObject.Parse(result);
                if (root["value"] != null)
                    data.Merge(root["value"]);
                url = root["@odata.nextLink"]?.ToString();
            } while (!string.IsNullOrEmpty(url));

            return data;
        }
    }
}

--- Helpers/ConfigLoader.cs ---

using Microsoft.Extensions.Configuration;
using System.IO;

namespace TeamsCDRDownloader.Helpers
{
    public static class ConfigLoader
    {
        public static IConfigurationRoot Config { get; private set; }

        static ConfigLoader()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            Config = builder.Build();
        }
    }
}

--- Helpers/Logger.cs ---

using System;
using System.IO;

namespace TeamsCDRDownloader.Helpers
{
    public static class Logger
    {
        private static string logPath;

        public static void Init()
        {
            logPath = Path.Combine("Logs", $"log_{DateTime.UtcNow:yyyyMMdd}.txt");
            Directory.CreateDirectory("Logs");
        }

        public static void LogInfo(string message)
        {
            File.AppendAllText(logPath, $"INFO: {DateTime.UtcNow}: {message}{Environment.NewLine}");
        }

        public static void LogError(string message)
        {
            File.AppendAllText(logPath, $"ERROR: {DateTime.UtcNow}: {message}{Environment.NewLine}");
        }
    }
}

--- appsettings.json ---

{
  "GraphAPI": {
    "ClientId": "your-client-id",
    "TenantId": "your-tenant-id",
    "ClientSecret": "your-client-secret",
    "Scope": "https://graph.microsoft.com/.default"
  }
}

--- TeamsCDRDownloader.csproj ---

<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.Configuration.Json" Version="6.0.0" />
    <PackageReference Include="Microsoft.Identity.Client" Version="4.54.0" />
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
  </ItemGroup>
</Project>

--- README.md ---

# Teams CDR Downloader

## Usage:
```
dotnet run -- 2025-05-31 "C:\\Input\\ConfId_20250531.csv"
```

--- END ---
