namespace Benchmark.Runner;

public sealed class ResourceMonitor : IDisposable
{
    private readonly string _containerName;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _samplerTask;

    private readonly List<double> _cpuSamples = new();
    private readonly List<double> _memorySamples = new();

    public double CpuAveragePercent { get; private set; }
    public double CpuMaxPercent { get; private set; }
    public double MemoryAverageMB { get; private set; }
    public double MemoryMaxMB { get; private set; }

    // Block I/O — delta durante o benchmark (não média de acumulado)
    private double? _initialDiskReadMb;
    private double? _initialDiskWriteMb;

    public double DiskReadMB { get; private set; }
    public double DiskWriteMB { get; private set; }
    public double DiskTotalMB { get; private set; }

    public int SampleCount
    {
        get { lock (_lock) return _cpuSamples.Count; }
    }

    public int MemorySampleCount
    {
        get { lock (_lock) return _memorySamples.Count; }
    }

    public ResourceMonitor(string containerName)
    {
        if (string.IsNullOrWhiteSpace(containerName))
            throw new ArgumentException("Nome do container não pode ser vazio.", nameof(containerName));

        _containerName = containerName;
    }

    public void Start()
    {
        // Captura Block I/O acumulado inicial (read/write) — snapshot antes do benchmark.
        // Block I/O é contador acumulado desde o início do container, então delta = final - inicial.
        try
        {
            var initial = DockerStatsProvider.TryGetCpuAndMemoryAsync(_containerName).GetAwaiter().GetResult();
            _initialDiskReadMb = initial.DiskReadMb;
            _initialDiskWriteMb = initial.DiskWriteMb;
        }
        catch { /* container ainda iniciando — delta ficará 0 */ }

        _samplerTask = Task.Run(() => SampleLoop(_cts.Token));
    }

    public void Stop()
    {
        _cts.Cancel();

        try
        {
            _samplerTask?.GetAwaiter().GetResult();
        }
        catch
        {
            // Loop interrompido pelo cancelamento.
        }

        // Captura Block I/O acumulado final e calcula delta do benchmark.
        try
        {
            var final = DockerStatsProvider.TryGetCpuAndMemoryAsync(_containerName).GetAwaiter().GetResult();
            double readDelta = 0, writeDelta = 0;
            if (_initialDiskReadMb.HasValue && final.DiskReadMb.HasValue)
                readDelta = Math.Max(0, final.DiskReadMb.Value - _initialDiskReadMb.Value);
            if (_initialDiskWriteMb.HasValue && final.DiskWriteMb.HasValue)
                writeDelta = Math.Max(0, final.DiskWriteMb.Value - _initialDiskWriteMb.Value);

            DiskReadMB = readDelta;
            DiskWriteMB = writeDelta;
            DiskTotalMB = readDelta + writeDelta;
        }
        catch { DiskReadMB = DiskWriteMB = DiskTotalMB = 0; }

        lock (_lock)
        {
            CpuAveragePercent = _cpuSamples.Count > 0 ? _cpuSamples.Average() : 0;
            CpuMaxPercent = _cpuSamples.Count > 0 ? _cpuSamples.Max() : 0;
            MemoryAverageMB = _memorySamples.Count > 0 ? _memorySamples.Average() : 0;
            MemoryMaxMB = _memorySamples.Count > 0 ? _memorySamples.Max() : 0;
        }
    }

    private async Task SampleLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            var stats = await DockerStatsProvider.TryGetCpuAndMemoryAsync(_containerName, token);

            lock (_lock)
            {
                if (stats.CpuPercent.HasValue)
                    _cpuSamples.Add(stats.CpuPercent.Value);
                if (stats.MemoryMb.HasValue)
                    _memorySamples.Add(stats.MemoryMb.Value);
                // Disco (Block I/O) não é amostrado em média/máximo — é delta inicial→final em Stop().
            }

            try
            {
                await Task.Delay(1000, token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}
