// Program.cs
using System;
using System.IO;
using System.Threading.Tasks;

namespace TeamsCdrDownloader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: TeamsCdrDownloader <date: yyyy-MM-dd> <conferenceIdCsvPath>");
                return;
            }

            string date = args[0];
            string csvPath = args[1];

            var configLoader = new ConfigLoader();
            var downloader = new CallRecordDownloader(configLoader);

            await downloader.ProcessConferenceIdsAsync(date, csvPath);

            Console.WriteLine("Processing completed.");
        }
    }
}

// GraphHelper.cs
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;

namespace TeamsCdrDownloader
{
    public static class GraphHelper
    {
        private static string AccessToken;
        private static DateTime TokenExpiry;
        private static readonly object TokenLock = new();

        public static async Task<string> GetAccessTokenAsync(GraphApiConfig config)
        {
            lock (TokenLock)
            {
                if (!string.IsNullOrEmpty(AccessToken) && TokenExpiry > DateTime.UtcNow.AddMinutes(5))
                    return AccessToken;
            }

            using var client = new HttpClient();
            var form = new MultipartFormDataContent
            {
                { new StringContent(config.ClientId), "client_id" },
                { new StringContent(config.ClientSecret), "client_secret" },
                { new StringContent("client_credentials"), "grant_type" },
                { new StringContent(config.Scope), "scope" }
            };

            var resp = await client.PostAsync($"https://login.microsoftonline.com/{config.TenantId}/oauth2/v2.0/token", form);
            var json = await resp.Content.ReadAsStringAsync();
            var doc = JsonDocument.Parse(json);
            lock (TokenLock)
            {
                AccessToken = doc.RootElement.GetProperty("access_token").GetString();
                TokenExpiry = DateTime.UtcNow.AddMinutes(55);
            }
            return AccessToken;
        }
    }
}

// CallRecordDownloader.cs
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Polly;

namespace TeamsCdrDownloader
{
    public class CallRecordDownloader
    {
        private readonly ConfigLoader _configLoader;
        private readonly HttpClient _httpClient;

        public CallRecordDownloader(ConfigLoader loader)
        {
            _configLoader = loader;
            _httpClient = new HttpClient();
        }

        public async Task ProcessConferenceIdsAsync(string date, string csvPath)
        {
            var records = LoadConferenceIds(csvPath);
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = _configLoader.Downloader.MaxDegreeOfParallelism };

            Directory.CreateDirectory(Path.Combine(_configLoader.Downloader.OutputRootPath, date.Replace("-", Path.DirectorySeparatorChar.ToString())));

            await Parallel.ForEachAsync(records, parallelOptions, async (confId, ct) =>
            {
                try
                {
                    await ProcessSingleConfIdAsync(date, confId);
                }
                catch (Exception ex)
                {
                    LogFailure(confId, ex.Message);
                }
            });
        }

        private List<string> LoadConferenceIds(string path)
        {
            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            return csv.GetRecords<dynamic>().Select(r => (string)r.ConferenceId).ToList();
        }

        private async Task ProcessSingleConfIdAsync(string date, string confId)
        {
            var token = await GraphHelper.GetAccessTokenAsync(_configLoader.GraphApi);
            var jsonRoot = await FetchCallRecordAsync(confId, token);
            jsonRoot["participants_v2"] = await FetchExpandedAsync(confId, "participants_v2", token);
            jsonRoot["sessions"] = await FetchExpandedAsync(confId, "sessions?$expand=segments", token);

            var outputPath = Path.Combine(_configLoader.Downloader.OutputRootPath, date.Replace("-", Path.DirectorySeparatorChar.ToString()), $"{confId}.json");
            await File.WriteAllTextAsync(outputPath, JsonSerializer.Serialize(jsonRoot, new JsonSerializerOptions { WriteIndented = true }));
        }

        private async Task<JsonElement> FetchCallRecordAsync(string confId, string token)
        {
            return await GetJsonWithRetryAsync($"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}", token);
        }

        private async Task<JsonElement> FetchExpandedAsync(string confId, string path, string token)
        {
            var results = new List<JsonElement>();
            var url = $"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}/{path}";
            while (!string.IsNullOrEmpty(url))
            {
                var json = await GetJsonWithRetryAsync(url, token);
                if (json.TryGetProperty("value", out var items))
                    results.AddRange(items.EnumerateArray());
                url = json.TryGetProperty("@odata.nextLink", out var next) ? next.GetString() : null;
            }
            return JsonDocument.Parse(JsonSerializer.Serialize(results)).RootElement;
        }

        private async Task<JsonElement> GetJsonWithRetryAsync(string url, string token)
        {
            var policy = Policy.Handle<Exception>().WaitAndRetryAsync(_configLoader.Downloader.MaxRetryAttempts,
                attempt => TimeSpan.FromSeconds(_configLoader.Downloader.RetryBackoffSeconds * attempt));

            return await policy.ExecuteAsync(async () =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
                var resp = await _httpClient.SendAsync(req);
                resp.EnsureSuccessStatusCode();
                var json = await resp.Content.ReadAsStringAsync();
                return JsonDocument.Parse(json).RootElement;
            });
        }

        private void LogFailure(string confId, string reason)
        {
            lock (this)
            {
                File.AppendAllText(_configLoader.Downloader.FailedCsvPath, $"{confId},\"{reason}\"{Environment.NewLine}");
            }
        }
    }
}

// ConfigLoader.cs
using Microsoft.Extensions.Configuration;
using System;

namespace TeamsCdrDownloader
{
    public class GraphApiConfig
    {
        public string TenantId { get; set; }
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string Scope { get; set; }
    }

    public class DownloaderConfig
    {
        public string OutputRootPath { get; set; }
        public string FailedCsvPath { get; set; }
        public string LogFilePath { get; set; }
        public int MaxDegreeOfParallelism { get; set; }
        public int ThrottleDelayMs { get; set; }
        public int MaxRetryAttempts { get; set; }
        public int RetryBackoffSeconds { get; set; }
    }

    public class ConfigLoader
    {
        public GraphApiConfig GraphApi { get; private set; }
        public DownloaderConfig Downloader { get; private set; }

        public ConfigLoader(string configFile = "appsettings.json")
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(configFile, optional: false, reloadOnChange: true)
                .Build();

            GraphApi = configuration.GetSection("GraphApi").Get<GraphApiConfig>();
            Downloader = configuration.GetSection("Downloader").Get<DownloaderConfig>();
        }
    }
}
