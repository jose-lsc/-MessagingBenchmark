namespace Benchmark.Contracts;

public class Scenario
{
    public int Id { get; set; }

    public int MessageCount { get; set; }

    public int MessageSizeBytes { get; set; }

    public int Producers { get; set; }

    public int Consumers { get; set; }

    public int MessagesPerSecond { get; set; }
}