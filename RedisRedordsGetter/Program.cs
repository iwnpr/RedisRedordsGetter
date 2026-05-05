using System.Xml.Linq;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/redis/extract-type2-guids", async ([FromServices] IConfiguration configuration, CancellationToken cancellationToken) =>
{
    var redisConnectionString = configuration["Redis"];
    if (string.IsNullOrWhiteSpace(redisConnectionString))
    {
        return Results.BadRequest("Redis connection string was not provided in configuration.");
    }

    var options = ConfigurationOptions.Parse(redisConnectionString);
    options.AllowAdmin = true;

    await using var connection = await ConnectionMultiplexer.ConnectAsync(options);
    var database = connection.GetDatabase();

    var endpoint = connection.GetEndPoints().First();
    var server = connection.GetServer(endpoint);

    var extractedRecords = new List<(string Guid, string TaskResultXml)>();

    await foreach (var key in server.KeysAsync(database.Database, pattern: "QBCH:dlrequest:*"))
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (await database.KeyTypeAsync(key) != RedisType.Hash)
        {
            continue;
        }

        var guid = ExtractGuidFromKey(key!);
        if (guid is null)
        {
            continue;
        }

        var taskResultXml = await database.HashGetAsync(key, "task_result_xml");
        if (taskResultXml.IsNullOrWhiteSpace)
        {
            continue;
        }

        extractedRecords.Add((guid, taskResultXml.ToString()));
    }

    var filteredGuids = extractedRecords
        .Where(record => HasType2Answer(record.TaskResultXml))
        .Select(record => record.Guid)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .OrderBy(guid => guid)
        .ToArray();

    var outputPath = Path.Combine(AppContext.BaseDirectory, "type2_guids.txt");
    await File.WriteAllLinesAsync(outputPath, filteredGuids, cancellationToken);

    return Results.Ok(new
    {
        TotalGuids = filteredGuids.Length,
        ExtractedRecords = extractedRecords.Count,
        OutputFile = outputPath
    });
});

app.Run();

static bool HasType2Answer(string xml)
{
    try
    {
        var document = XDocument.Parse(xml);
        var typeAttribute = document.Root
            ?.DescendantsAndSelf()
            .Attributes("ТипОтвета")
            .FirstOrDefault()?.Value;

        return string.Equals(typeAttribute, "2", StringComparison.Ordinal);
    }
    catch
    {
        return false;
    }
}

static string? ExtractGuidFromKey(RedisKey key)
{
    var keyString = key.ToString();
    const string prefix = "QBCH:dlrequest:";

    if (!keyString.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
    {
        return null;
    }

    var tail = keyString[prefix.Length..];
    var guidPart = tail.Split(':', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();

    return Guid.TryParse(guidPart, out var parsedGuid)
        ? parsedGuid.ToString()
        : null;
}
