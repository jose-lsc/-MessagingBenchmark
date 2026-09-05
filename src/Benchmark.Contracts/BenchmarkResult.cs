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

    // Block I/O durante o benchmark (delta, não acumulado) — em MB
    public double DiskReadMB { get; set; }
    public double DiskWriteMB { get; set; }
    public double DiskTotalMB { get; set; }

    [Obsolete("DiskAverageMB era média de contador acumulado e foi substituído por DiskTotalMB/Read/Write (delta).")]
    public double DiskAverageMB { get; set; }
    [Obsolete("DiskMaxMB era máximo de contador acumulado e foi substituído por DiskTotalMB/Read/Write (delta).")]
    public double DiskMaxMB { get; set; }
}