// Program.cs
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace TeamsCDRDownloader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: TeamsCDRDownloader <date> <csvFilePath>");
                return;
            }

            string date = args[0];
            string csvPath = args[1];

            var config = ConfigLoader.LoadConfiguration();
            var downloader = new CallRecordDownloader(config);
            await downloader.ProcessConferenceIdsAsync(date, csvPath);
        }
    }
}

// ConfigLoader.cs
using Microsoft.Extensions.Configuration;

namespace TeamsCDRDownloader
{
    public static class ConfigLoader
    {
        public static IConfiguration LoadConfiguration()
        {
            return new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();
        }
    }
}

// GraphHelper.cs
using Microsoft.Identity.Client;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System;
using Polly;

namespace TeamsCDRDownloader
{
    public class GraphHelper
    {
        private readonly IConfiguration _config;
        private string _accessToken;
        private DateTime _tokenExpiry;
        private static readonly HttpClient client = new HttpClient();

        public GraphHelper(IConfiguration config)
        {
            _config = config;
            _accessToken = string.Empty;
            _tokenExpiry = DateTime.MinValue;
        }

        public async Task<string> GetAccessTokenAsync()
        {
            if (DateTime.UtcNow >= _tokenExpiry)
            {
                var app = ConfidentialClientApplicationBuilder.Create(_config["AzureAd:ClientId"])
                    .WithClientSecret(_config["AzureAd:ClientSecret"])
                    .WithTenantId(_config["AzureAd:TenantId"])
                    .Build();

                var result = await app.AcquireTokenForClient(new[] { "https://graph.microsoft.com/.default" }).ExecuteAsync();
                _accessToken = result.AccessToken;
                _tokenExpiry = DateTime.UtcNow.AddMinutes(50);
            }
            return _accessToken;
        }

        public async Task<string> GetGraphDataAsync(string url)
        {
            var token = await GetAccessTokenAsync();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            return await Policy
                .Handle<HttpRequestException>()
                .Or<TaskCanceledException>()
                .WaitAndRetryAsync(5, retry => TimeSpan.FromSeconds(Math.Pow(2, retry)))
                .ExecuteAsync(async () =>
                {
                    var response = await client.GetAsync(url);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadAsStringAsync();
                });
        }
    }
}

// CallRecordDownloader.cs
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

namespace TeamsCDRDownloader
{
    public class CallRecordDownloader
    {
        private readonly IConfiguration _config;
        private readonly GraphHelper _graphHelper;

        public CallRecordDownloader(IConfiguration config)
        {
            _config = config;
            _graphHelper = new GraphHelper(config);
        }

        public async Task ProcessConferenceIdsAsync(string date, string csvFilePath)
        {
            var records = ReadConferenceIds(csvFilePath);
            var options = new ParallelOptions { MaxDegreeOfParallelism = 4 }; // For 4 cores
            var failed = new List<string>();
            var outputRoot = Path.Combine("Output", date);

            Directory.CreateDirectory(outputRoot);

            await Task.Run(() =>
            {
                Parallel.ForEach(records, options, confId =>
                {
                    try
                    {
                        var json = FetchCallRecordWithExpansionsAsync(confId).Result;
                        var outPath = Path.Combine(outputRoot, confId + ".json");
                        File.WriteAllText(outPath, json);
                    }
                    catch (Exception ex)
                    {
                        lock (failed)
                        {
                            failed.Add(confId);
                        }
                        Console.WriteLine($"Failed: {confId} - {ex.Message}");
                    }
                });
            });

            if (failed.Count > 0)
            {
                File.WriteAllLines(Path.Combine(outputRoot, "Failed.csv"), failed);
            }
        }

        private List<string> ReadConferenceIds(string csvFilePath)
        {
            using var reader = new StreamReader(csvFilePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            return csv.GetRecords<dynamic>().Select(r => (string)r.ConferenceId).ToList();
        }

        private async Task<string> FetchCallRecordWithExpansionsAsync(string confId)
        {
            string baseUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{confId}";
            string callRecordJson = await _graphHelper.GetGraphDataAsync(baseUrl);
            
            var callRecord = JObject.Parse(callRecordJson);

            string participantsUrl = baseUrl + "/participants_v2";
            callRecord["participants_v2"] = await FetchPagedDataAsync(participantsUrl);

            string sessionsUrl = baseUrl + "/sessions?$expand=segments";
            callRecord["sessions"] = await FetchPagedDataAsync(sessionsUrl);

            return callRecord.ToString();
        }

        private async Task<JArray> FetchPagedDataAsync(string url)
        {
            var results = new JArray();
            while (!string.IsNullOrEmpty(url))
            {
                var json = await _graphHelper.GetGraphDataAsync(url);
                var obj = JObject.Parse(json);
                results.Merge(obj["value"]);
                url = (string)obj["@odata.nextLink"];
            }
            return results;
        }
    }
}
