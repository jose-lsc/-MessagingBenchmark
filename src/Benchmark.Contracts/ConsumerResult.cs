namespace Benchmark.Contracts;

public class ConsumerResult
{
    public int ReceivedCount { get; set; }

    public List<double> LatenciesMs { get; set; } = new();
}
