using System.Diagnostics;

namespace Benchmark.Runner;

public sealed class ResourceMonitor : IDisposable
{
    private readonly Process _process = Process.GetCurrentProcess();
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts = new();

    private TimeSpan _lastCpuTime;
    private DateTime _lastSampleAt;
    private Task? _samplerTask;

    private readonly List<double> _cpuSamples = new();
    private readonly List<double> _memorySamples = new();

    public double CpuAveragePercent { get; private set; }
    public double CpuMaxPercent { get; private set; }
    public double MemoryAverageMB { get; private set; }
    public double MemoryMaxMB { get; private set; }

    public void Start()
    {
        _process.Refresh();

        _lastCpuTime = _process.TotalProcessorTime;
        _lastSampleAt = DateTime.UtcNow;

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
            // Loop de amostragem interrompido pelo cancelamento.
        }

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
        while (true)
        {
            try
            {
                await Task.Delay(250, token);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            _process.Refresh();

            var now = DateTime.UtcNow;

            var currentCpuTime = _process.TotalProcessorTime;

            double cpuPercent =
                (currentCpuTime - _lastCpuTime).TotalMilliseconds
                / (now - _lastSampleAt).TotalMilliseconds
                * 100.0;

            cpuPercent /= Environment.ProcessorCount;

            _lastCpuTime = currentCpuTime;
            _lastSampleAt = now;

            double memoryMb = _process.WorkingSet64 / (1024.0 * 1024.0);

            lock (_lock)
            {
                _cpuSamples.Add(cpuPercent);
                _memorySamples.Add(memoryMb);
            }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
    }
}
