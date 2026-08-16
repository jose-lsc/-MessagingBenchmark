namespace Benchmark.Contracts;

public class BenchmarkResult
{
    public int ScenarioId { get; set; }
    public int MessageCount { get; set; }
    public int MessageSizeBytes { get; set; }
    public int Producers { get; set; }
    public int Consumers { get; set; }
    public int MessagesPerSecond { get; set; }

    public double TotalSeconds { get; set; }
    public double Throughput { get; set; }

    public double AverageLatencyMs { get; set; }
    public double MinLatencyMs { get; set; }
    public double MaxLatencyMs { get; set; }
    public double P95LatencyMs { get; set; }

    public double CpuAveragePercent { get; set; }
    public double CpuMaxPercent { get; set; }
    public double MemoryAverageMB { get; set; }
    public double MemoryMaxMB { get; set; }
}