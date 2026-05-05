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

    var filteredGuids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    await foreach (var key in server.KeysAsync(database.Database, pattern: "QBCH:dlrequest:*"))
    {
        cancellationToken.ThrowIfCancellationRequested();

        var hashEntries = await database.HashGetAllAsync(key);
        if (hashEntries.Length == 0)
        {
            continue;
        }

        var entryMap = hashEntries.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString(), StringComparer.OrdinalIgnoreCase);
        if (!entryMap.TryGetValue("task_result_xml", out var taskResultXml) || string.IsNullOrWhiteSpace(taskResultXml))
        {
            continue;
        }

        if (!HasType2Answer(taskResultXml))
        {
            continue;
        }

        var guid = ExtractGuidFromKey(key!);
        if (guid is not null)
        {
            filteredGuids.Add(guid);
        }
    }

    var outputPath = Path.Combine(AppContext.BaseDirectory, "type2_guids.txt");
    await File.WriteAllLinesAsync(outputPath, filteredGuids.OrderBy(x => x), cancellationToken);

    return Results.Ok(new
    {
        TotalGuids = filteredGuids.Count,
        OutputFile = outputPath
    });
});

app.Run();

static bool HasType2Answer(string xml)
{
    try
    {
        var document = XDocument.Parse(xml);
        var typeAttribute = document
            .DescendantsAndSelf()
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
