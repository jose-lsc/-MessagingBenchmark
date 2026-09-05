using System.Diagnostics;
using System.Globalization;

namespace Benchmark.Runner;

internal static class DockerStatsProvider
{
    public readonly record struct ContainerStats(double? CpuPercent, double? MemoryMb, double? DiskReadMb, double? DiskWriteMb)
    {
        public double? DiskMb => DiskReadMb.HasValue || DiskWriteMb.HasValue
            ? (DiskReadMb ?? 0) + (DiskWriteMb ?? 0)
            : null;
    }

    public static Task<double?> TryGetCpuPercentAsync(string containerName, CancellationToken token = default)
        => TryGetCpuAndMemoryAsync(containerName, token).ContinueWith(t =>
            t.IsCompletedSuccessfully ? t.Result.CpuPercent : null, token);

    public static Task<double?> TryGetMemoryMbAsync(string containerName, CancellationToken token = default)
        => TryGetCpuAndMemoryAsync(containerName, token).ContinueWith(t =>
            t.IsCompletedSuccessfully ? t.Result.MemoryMb : null, token);

    public static Task<double?> TryGetDiskMbAsync(string containerName, CancellationToken token = default)
        => TryGetCpuAndMemoryAsync(containerName, token).ContinueWith(t =>
            t.IsCompletedSuccessfully ? t.Result.DiskMb : null, token);

    public static async Task<ContainerStats> TryGetCpuAndMemoryAsync(string containerName, CancellationToken token = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = $"stats --no-stream --format \"{{{{.CPUPerc}}}}|{{{{.MemUsage}}}}|{{{{.BlockIO}}}}\" {containerName}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = new Process { StartInfo = psi };
            process.Start();

            string output = await process.StandardOutput.ReadToEndAsync(cts.Token);
            string error = await process.StandardError.ReadToEndAsync(cts.Token);
            await process.WaitForExitAsync(cts.Token);

            if (process.ExitCode != 0)
            {
                Debug.WriteLine($"docker stats erro ({containerName}): {error.Trim()}");
                return new ContainerStats(null, null, null, null);
            }

            // Ex: "2.34%|123.45MiB / 2GiB|1.2MB / 800kB"
            var parts = output.Trim().Split('|');
            double? cpu = null;
            double? memMb = null;
            double? diskReadMb = null;
            double? diskWriteMb = null;

            if (parts.Length >= 1)
            {
                var rawCpu = parts[0].Trim().TrimEnd('%').Trim().Replace(',', '.');
                if (double.TryParse(rawCpu, NumberStyles.Float, CultureInfo.InvariantCulture, out double v))
                    cpu = v;
            }

            if (parts.Length >= 2)
            {
                // MemUsage contém "X unit / Y unit" — pega apenas uso
                var memUsage = parts[1].Trim(); // "123.45MiB / 2GiB"
                var usagePart = memUsage.Split('/')[0].Trim(); // "123.45MiB"
                memMb = ParseDockerMemoryToMb(usagePart);
            }

            if (parts.Length >= 3)
            {
                // BlockIO contém "read / write" ex: "1.2MB / 800kB" ou "0B / 0B" — acumulado desde o início do container
                var blockIo = parts[2].Trim();
                var (read, write) = ParseDockerBlockIoToReadWriteMb(blockIo);
                diskReadMb = read;
                diskWriteMb = write;
            }

            return new ContainerStats(cpu, memMb, diskReadMb, diskWriteMb);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return new ContainerStats(null, null, null, null);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"DockerStatsProvider erro: {ex.Message}");
            return new ContainerStats(null, null, null, null);
        }
    }

    private static double? ParseDockerMemoryToMb(string value)
    {
        // value ex: "123.45MiB", "1.2GiB", "800KiB", "512B"
        if (string.IsNullOrWhiteSpace(value)) return null;
        value = value.Trim();
        string numPart = "";
        string unitPart = "";
        foreach (char c in value)
        {
            if (char.IsDigit(c) || c == '.' || c == ',' || c == '-')
                numPart += c;
            else if (!char.IsWhiteSpace(c))
                unitPart += c;
        }
        numPart = numPart.Replace(',', '.');
        if (!double.TryParse(numPart, NumberStyles.Float, CultureInfo.InvariantCulture, out double num))
            return null;

        unitPart = unitPart.Trim().ToUpperInvariant(); // MIB, GIB, KIB, B, MB...
        return unitPart switch
        {
            "B" => num / (1024 * 1024),
            "KB" or "KIB" => num / 1024,
            "MB" or "MIB" => num,
            "GB" or "GIB" => num * 1024,
            "TB" or "TIB" => num * 1024 * 1024,
            _ => null
        };
    }

    private static double? ParseDockerBlockIoToMb(string blockIo)
    {
        var (read, write) = ParseDockerBlockIoToReadWriteMb(blockIo);
        if (read.HasValue || write.HasValue) return (read ?? 0) + (write ?? 0);
        return null;
    }

    private static (double? readMb, double? writeMb) ParseDockerBlockIoToReadWriteMb(string blockIo)
    {
        // blockIo ex: "1.2MB / 800kB", "12.3MiB / 4.5MiB", "0B / 0B"
        if (string.IsNullOrWhiteSpace(blockIo)) return (null, null);
        var parts = blockIo.Split('/');
        double? read = null;
        double? write = null;
        if (parts.Length >= 1) read = ParseDockerMemoryToMb(parts[0].Trim());
        if (parts.Length >= 2) write = ParseDockerMemoryToMb(parts[1].Trim());
        return (read, write);
    }
}
