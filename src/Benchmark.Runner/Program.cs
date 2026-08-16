using System.Diagnostics;
using System.Globalization;
using Benchmark.Brokers;
using Benchmark.Contracts;
using Benchmark.Runner;

var scenarios = new List<Scenario>
{
    new() { Id = 1, MessageCount = 50_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 2, MessageCount = 50_000, MessageSizeBytes = 100, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 3, MessageCount = 25_000, MessageSizeBytes = 10_000, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 4, MessageCount = 5_000, MessageSizeBytes = 100_000, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 5, MessageCount = 10_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 6, MessageCount = 150_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 7, MessageCount = 100_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 4, MessagesPerSecond = 0 },
    new() { Id = 8, MessageCount = 100_000, MessageSizeBytes = 1_000, Producers = 4, Consumers = 1, MessagesPerSecond = 0 },
    new() { Id = 9, MessageCount = 100_000, MessageSizeBytes = 1_000, Producers = 4, Consumers = 4, MessagesPerSecond = 0 },
    new() { Id = 10, MessageCount = 30_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 1, MessagesPerSecond = 2_000 },
    new() { Id = 11, MessageCount = 40_000, MessageSizeBytes = 1_000, Producers = 1, Consumers = 1, MessagesPerSecond = 8_000 }
};

var brokers = new (string Key, string Name, Func<string, IBenchmarkBroker> Factory)[]
{
    ("rabbitmq", "RabbitMQ", name => new RabbitMqBroker(name)),
    ("kafka", "Kafka", name => new KafkaBroker(name)),
    ("pulsar", "Pulsar", name => new PulsarBroker(name))
};

// Filtros opcionais por argumentos:
//   dotnet run --project src/Benchmark.Runner -- rabbitmq 1 3
var brokerFilter = args
    .Where(a => brokers.Any(b => b.Key.Equals(a, StringComparison.OrdinalIgnoreCase)))
    .Select(a => a.ToLowerInvariant())
    .ToHashSet();

var scenarioFilter = args
    .Where(a => int.TryParse(a, out _))
    .Select(int.Parse)
    .ToHashSet();

var selectedBrokers = brokerFilter.Count == 0
    ? brokers
    : brokers.Where(b => brokerFilter.Contains(b.Key));

var selectedScenarios = scenarioFilter.Count == 0
    ? scenarios
    : scenarios.Where(s => scenarioFilter.Contains(s.Id));

foreach (var (key, brokerName, factory) in selectedBrokers)
{
    Console.WriteLine();
    Console.WriteLine("########################################");
    Console.WriteLine($"# BROKER: {brokerName}");
    Console.WriteLine("########################################");

    var results = new List<string>
    {
        "Broker,Cenario,Mensagens,TamanhoBytes,Producers,Consumers,TaxaMsgSeg,"
            + "TempoSegundos,ThroughputMsgSeg,LatenciaMediaMs,LatenciaMinMs,"
            + "LatenciaMaxMs,LatenciaP95Ms,CpuMediaPct,CpuMaxPct,MemoriaMediaMB,MemoriaMaxMB"
    };

    foreach (var scenario in selectedScenarios)
    {
        // Nome único de fila/tópico por cenário: evita lixo entre cenários
        // (Kafka e Pulsar não têm "purge" simples como o Rabbit).
        string topicName =
            $"benchmark-{key}-{scenario.Id}-{Guid.NewGuid():N}";

        Console.WriteLine();
        Console.WriteLine("======================================");
        Console.WriteLine($"{brokerName.ToUpperInvariant()} | CENÁRIO {scenario.Id}");
        Console.WriteLine("======================================");

        Console.WriteLine($"Mensagens : {scenario.MessageCount}");
        Console.WriteLine($"Tamanho   : {scenario.MessageSizeBytes} bytes");
        Console.WriteLine($"Producers : {scenario.Producers}");
        Console.WriteLine($"Consumers : {scenario.Consumers}");
        Console.WriteLine($"Taxa      : {(scenario.MessagesPerSecond == 0
            ? "Máxima"
            : $"{scenario.MessagesPerSecond} msg/s")}");

        var broker = factory(topicName);

        // 1. Prepara a topologia (fila/tópico) para o cenário.
        await broker.PrepareAsync(scenario.Consumers);

        // 2. Inscreve os consumers e devolve uma Task que completa
        //    quando todas as mensagens esperadas forem consumidas.
        Console.WriteLine();
        Console.WriteLine("Iniciando Consumers...");

        var consumptionTask = ConsumerRunner.StartConsumers(
            scenario.Consumers,
            scenario.MessageCount,
            broker);

        // 3. Liga o monitor de CPU e memória do processo.
        using var resources = new ResourceMonitor();
        resources.Start();

        // 4. Produz as mensagens (pipeline completo: produção + consumo).
        Console.WriteLine("Iniciando Producers...");

        var stopwatch = Stopwatch.StartNew();

        await ProducerRunner.RunAsync(scenario, broker);

        var consumerResult = await consumptionTask;

        stopwatch.Stop();

        // 5. Encerra a amostragem de recursos.
        resources.Stop();

        // 6. Calcula as métricas dos pilares.
        double totalSeconds = stopwatch.Elapsed.TotalSeconds;

        double throughput = scenario.MessageCount / totalSeconds;

        var (avgLatency, minLatency, maxLatency, p95Latency) =
            ComputeLatencyStats(consumerResult.LatenciesMs);

        var benchmarkResult = new BenchmarkResult
        {
            ScenarioId = scenario.Id,
            MessageCount = scenario.MessageCount,
            MessageSizeBytes = scenario.MessageSizeBytes,
            Producers = scenario.Producers,
            Consumers = scenario.Consumers,
            MessagesPerSecond = scenario.MessagesPerSecond,
            TotalSeconds = totalSeconds,
            Throughput = throughput,
            AverageLatencyMs = avgLatency,
            MinLatencyMs = minLatency,
            MaxLatencyMs = maxLatency,
            P95LatencyMs = p95Latency,
            CpuAveragePercent = resources.CpuAveragePercent,
            CpuMaxPercent = resources.CpuMaxPercent,
            MemoryAverageMB = resources.MemoryAverageMB,
            MemoryMaxMB = resources.MemoryMaxMB
        };

        results.Add(FormatCsv(benchmarkResult));

        PrintSummary(benchmarkResult);

        Console.WriteLine();
        Console.WriteLine($"Cenário {scenario.Id} finalizado.");

        // Pequeno intervalo antes do próximo cenário.
        await Task.Delay(1000);
    }

    string csvPath = Path.Combine(
        Directory.GetCurrentDirectory(),
        $"{key}-results.csv"
    );

    await File.WriteAllLinesAsync(csvPath, results);

    Console.WriteLine();
    Console.WriteLine($"Resultados salvos em: {csvPath}");
}

Console.WriteLine();
Console.WriteLine("======================================");
Console.WriteLine("TODOS OS BROKERS FORAM EXECUTADOS");
Console.WriteLine("======================================");


static (double avg, double min, double max, double p95) ComputeLatencyStats(
    IReadOnlyList<double> latencies)
{
    if (latencies.Count == 0)
    {
        return (0, 0, 0, 0);
    }

    var sorted = latencies.OrderBy(x => x).ToArray();

    double avg = latencies.Average();
    double min = sorted[0];
    double max = sorted[^1];

    int index = (int)Math.Ceiling(sorted.Length * 0.95) - 1;

    double p95 = sorted[index];

    return (avg, min, max, p95);
}

static string FormatCsv(BenchmarkResult result)
{
    return string.Join(",",
        "RabbitMQ",
        result.ScenarioId,
        result.MessageCount,
        result.MessageSizeBytes,
        result.Producers,
        result.Consumers,
        result.MessagesPerSecond,
        result.TotalSeconds.ToString("F4", CultureInfo.InvariantCulture),
        result.Throughput.ToString("F2", CultureInfo.InvariantCulture),
        result.AverageLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
        result.MinLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
        result.MaxLatencyMs.ToString("F2", CultureInfo.InvariantCulture),
        result.P95LatencyMs.ToString("F2", CultureInfo.InvariantCulture),
        result.CpuAveragePercent.ToString("F2", CultureInfo.InvariantCulture),
        result.CpuMaxPercent.ToString("F2", CultureInfo.InvariantCulture),
        result.MemoryAverageMB.ToString("F2", CultureInfo.InvariantCulture),
        result.MemoryMaxMB.ToString("F2", CultureInfo.InvariantCulture));
}

static void PrintSummary(BenchmarkResult result)
{
    Console.WriteLine();
    Console.WriteLine($"Tempo total : {result.TotalSeconds:F2} s");
    Console.WriteLine($"Throughput  : {result.Throughput:F2} msg/s");
    Console.WriteLine(
        $"Latência    : média {result.AverageLatencyMs:F2} ms | " +
        $"mín {result.MinLatencyMs:F2} ms | máx {result.MaxLatencyMs:F2} ms | " +
        $"p95 {result.P95LatencyMs:F2} ms");
    Console.WriteLine(
        $"CPU         : média {result.CpuAveragePercent:F1}% | " +
        $"máx {result.CpuMaxPercent:F1}%");
    Console.WriteLine(
        $"Memória     : média {result.MemoryAverageMB:F1} MB | " +
        $"máx {result.MemoryMaxMB:F1} MB");
}
