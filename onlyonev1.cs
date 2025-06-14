// ------------------------- ConfigLoader.cs -------------------------
using Microsoft.Extensions.Configuration;

public class ConfigLoader
{
    public IConfigurationRoot Configuration { get; }

    public ConfigLoader(string path = "appsettings.json")
    {
        Configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(path, optional: false, reloadOnChange: true)
            .Build();
    }
}

// ------------------------- GraphHelper.cs -------------------------
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text;

public static class GraphHelper
{
    private static string? AccessToken = null;
    private static DateTime TokenExpiry = DateTime.MinValue;
    private static readonly object TokenLock = new();

    public static async Task<string> GetAccessTokenAsync(IConfiguration config)
    {
        lock (TokenLock)
        {
            if (AccessToken != null && DateTime.UtcNow < TokenExpiry)
                return AccessToken;
        }

        using var client = new HttpClient();
        var dict = new Dictionary<string, string>
        {
            {"client_id", config["GraphApi:ClientId"] },
            {"scope", config["GraphApi:Scope"] },
            {"client_secret", config["GraphApi:ClientSecret"] },
            {"grant_type", "client_credentials" }
        };

        var res = await client.PostAsync($"https://login.microsoftonline.com/{config["GraphApi:TenantId"]}/oauth2/v2.0/token", new FormUrlEncodedContent(dict));
        var json = await res.Content.ReadAsStringAsync();
        var token = JsonDocument.Parse(json).RootElement;

        lock (TokenLock)
        {
            AccessToken = token.GetProperty("access_token").GetString();
            TokenExpiry = DateTime.UtcNow.AddMinutes(50);
            return AccessToken!;
        }
    }

    public static async Task<string> GetApiResponseAsync(HttpClient client, string url)
    {
        var res = await client.GetAsync(url);
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadAsStringAsync();
    }
}

// ------------------------- CallRecordDownloader.cs -------------------------
using System.Text.Json.Nodes;
using Newtonsoft.Json.Linq;
using Polly;
using CsvHelper;
using System.Globalization;

public class CallRecordDownloader
{
    private readonly IConfiguration _config;
    private readonly HttpClient _httpClient;
    private readonly string _outputRoot;
    private readonly string _failedCsv;
    private readonly int _maxParallel;

    public CallRecordDownloader(IConfiguration config)
    {
        _config = config;
        _httpClient = new HttpClient();
        _outputRoot = config["Downloader:OutputRootPath"]!;
        _failedCsv = config["Downloader:FailedCsvPath"]!;
        _maxParallel = int.Parse(config["Downloader:MaxDegreeOfParallelism"]!);
    }

    public async Task ProcessConferenceIdsAsync(IEnumerable<string> ids)
    {
        var policy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(int.Parse(_config["Downloader:MaxRetryAttempts"]!),
                attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        Directory.CreateDirectory(_outputRoot);

        await Parallel.ForEachAsync(ids, new ParallelOptions { MaxDegreeOfParallelism = _maxParallel }, async (id, ct) =>
        {
            await policy.ExecuteAsync(async () =>
            {
                var token = await GraphHelper.GetAccessTokenAsync(_config);
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var recordUrl = $"https://graph.microsoft.com/v1.0/communications/callRecords/{id}";
                var json = JObject.Parse(await GraphHelper.GetApiResponseAsync(_httpClient, recordUrl));

                var participants = await GetPagedDataAsync($"{recordUrl}/participants_v2");
                var sessions = await GetPagedDataAsync($"{recordUrl}/sessions?$expand=segments");

                json["participants_v2"] = JArray.FromObject(participants);
                json["sessions"] = JArray.FromObject(sessions);

                var dt = DateTime.UtcNow;
                var outDir = Path.Combine(_outputRoot, dt.Year.ToString(), dt.ToString("MMM"), dt.ToString("dd"));
                Directory.CreateDirectory(outDir);

                var outPath = Path.Combine(outDir, $"{id}.json");
                await File.WriteAllTextAsync(outPath, json.ToString());
            });
        });
    }

    private async Task<List<JToken>> GetPagedDataAsync(string url)
    {
        var data = new List<JToken>();
        string? nextLink = url;

        while (!string.IsNullOrEmpty(nextLink))
        {
            var response = await GraphHelper.GetApiResponseAsync(_httpClient, nextLink);
            var root = JObject.Parse(response);
            if (root["value"] != null)
                data.AddRange(root["value"]);

            nextLink = root["@odata.nextLink"]?.ToString();
        }
        return data;
    }
}

// ------------------------- Program.cs -------------------------
var config = new ConfigLoader().Configuration;
var csvFile = args[1];
var lines = File.ReadAllLines(csvFile).Skip(1); // skip header
var ids = lines.Select(l => l.Split(',')[0]);

var downloader = new CallRecordDownloader(config);
await downloader.ProcessConferenceIdsAsync(ids);

Console.WriteLine("Download complete.");
