using System;
using System.Collections.Generic;
using System.Text;

namespace Benchmark.Contracts
{
    public class Message
    {
        public long Id { get; set; }

        public DateTime Timestamp { get; set; }

        public string Payload { get; set; } = string.Empty;
    }
}

