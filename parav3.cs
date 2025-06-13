// appsettings.json { "Graph": { "TenantId": "<Your-Tenant-Id>", "ClientId": "<Your-Client-Id>", "ClientSecret": "<Your-Client-Secret>" }, "Paths": { "InputCsvFolder": "InputCsv", "OutputJsonFolder": "OutputJson", "FailedCsvPath": "FailedCalls.csv" }, "Logging": { "LogFilePath": "log.txt" }, "Parallelism": { "MaxDegreeOfParallelism": 4 } }

// Program.cs using System; using System.IO; using System.Threading.Tasks; using Microsoft.Extensions.Configuration; using Serilog;

namespace TeamsGraphApiCaller { class Program { static async Task Main(string[] args) { IConfiguration config = new ConfigurationBuilder() .SetBasePath(Directory.GetCurrentDirectory()) .AddJsonFile("appsettings.json", optional: false) .Build();

string logPath = config["Logging:LogFilePath"] ?? "log.txt";

        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File(logPath, rollingInterval: RollingInterval.Day)
            .CreateLogger();

        var processor = new ConferenceIdProcessor(config);
        await processor.ProcessConferenceIdsAsync();

        Log.CloseAndFlush();
    }
}

}

// GraphHelper.cs using System; using System.Net.Http; using System.Threading.Tasks; using Microsoft.Extensions.Configuration; using Microsoft.Identity.Client;

namespace TeamsGraphApiCaller { public class GraphHelper { private readonly string tenantId, clientId, clientSecret; private readonly IConfidentialClientApplication app; private readonly string[] scopes = new[] { "https://graph.microsoft.com/.default" }; private string accessToken; private DateTime tokenExpiry = DateTime.MinValue;

public GraphHelper(IConfiguration config)
    {
        tenantId = config["Graph:TenantId"];
        clientId = config["Graph:ClientId"];
        clientSecret = config["Graph:ClientSecret"];

        app = ConfidentialClientApplicationBuilder.Create(clientId)
            .WithClientSecret(clientSecret)
            .WithAuthority(new Uri($"https://login.microsoftonline.com/{tenantId}"))
            .Build();
    }

    public async Task<string> GetAccessTokenAsync()
    {
        if (DateTime.UtcNow < tokenExpiry)
            return accessToken;

        var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();
        accessToken = result.AccessToken;
        tokenExpiry = DateTime.UtcNow.AddMinutes(50);
        return accessToken;
    }
}

}

// ConferenceIdProcessor.cs using System; using System.Collections.Concurrent; using System.Globalization; using System.IO; using System.Linq; using System.Threading.Tasks; using CsvHelper; using Microsoft.Extensions.Configuration; using Newtonsoft.Json.Linq; using Polly; using Serilog;

namespace TeamsGraphApiCaller { public class ConferenceIdProcessor { private readonly IConfiguration config; private readonly GraphHelper graphHelper; private readonly string inputFolder; private readonly string outputFolder; private readonly string failedCsv; private readonly int maxParallelism;

public ConferenceIdProcessor(IConfiguration config)
    {
        this.config = config;
        this.graphHelper = new GraphHelper(config);
        this.inputFolder = config["Paths:InputCsvFolder"];
        this.outputFolder = config["Paths:OutputJsonFolder"];
        this.failedCsv = config["Paths:FailedCsvPath"];
        this.maxParallelism = int.Parse(config["Parallelism:MaxDegreeOfParallelism"]);
    }

    public async Task ProcessConferenceIdsAsync()
    {
        var allFiles = Directory.GetFiles(inputFolder, "*.csv");
        var conferenceIds = allFiles.SelectMany(file => CsvReaderHelper.ReadConferenceIds(file)).Distinct().ToList();
        var failedBag = new ConcurrentBag<string>();

        await Parallel.ForEachAsync(conferenceIds, new ParallelOptions { MaxDegreeOfParallelism = maxParallelism }, async (id, token) =>
        {
            try
            {
                string jsonPath = Path.Combine(outputFolder, DateTime.UtcNow.ToString("yyyy/MMM/dd"), $"{id}.json");
                Directory.CreateDirectory(Path.GetDirectoryName(jsonPath));

                if (File.Exists(jsonPath))
                {
                    Log.Information($"Skipping existing JSON: {jsonPath}");
                    return;
                }

                var policy = Policy.Handle<Exception>().WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    (ex, ts, count, ctx) => Log.Warning($"Retry {count} for {id}: {ex.Message}"));

                await policy.ExecuteAsync(async () =>
                {
                    var accessToken = await graphHelper.GetAccessTokenAsync();
                    var resultJson = await GraphApiFetcher.FetchCallDetailsAsync(id, accessToken);
                    await File.WriteAllTextAsync(jsonPath, resultJson);
                    Log.Information($"Saved JSON for {id}");
                });
            }
            catch (Exception ex)
            {
                Log.Error($"Failed for {id}: {ex.Message}");
                failedBag.Add($"{id},{ex.Message}");
            }
        });

        await File.WriteAllLinesAsync(failedCsv, failedBag);
    }
}

}

// CsvReaderHelper.cs using System.Collections.Generic; using System.Globalization; using System.IO; using CsvHelper;

namespace TeamsGraphApiCaller { public static class CsvReaderHelper { public static IEnumerable<string> ReadConferenceIds(string path) { using var reader = new StreamReader(path); using var csv = new CsvReader(reader, CultureInfo.InvariantCulture); while (csv.Read()) { var id = csv.GetField(0); if (!string.IsNullOrWhiteSpace(id)) yield return id; } } } }

// GraphApiFetcher.cs using System.Net.Http; using System.Net.Http.Headers; using System.Threading.Tasks; using Newtonsoft.Json.Linq;

namespace TeamsGraphApiCaller { public static class GraphApiFetcher { public static async Task<string> FetchCallDetailsAsync(string conferenceId, string token) { using var client = new HttpClient(); client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

var url = $"https://graph.microsoft.com/v1.0/communications/callRecords/{conferenceId}?$expand=sessions($expand=segments),participants_v2";
        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var json = JObject.Parse(content);
        return json.ToString();
    }
}

}

