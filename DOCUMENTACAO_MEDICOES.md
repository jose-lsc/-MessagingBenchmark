# Documentação das Medições do Benchmark

## Visão Geral

Este documento descreve como as métricas coletadas no arquivo CSV são obtidas durante a execução do benchmark, e como cada uma se relaciona com os três pilares fundamentais de mensageria: **Throughput**, **Latência** e **Uso de Recursos**.

---

## Estrutura do CSV

O arquivo CSV gerado contém as seguintes colunas:

| Coluna | Descrição |
|--------|-----------|
| Broker | Nome do broker testado (RabbitMQ, Kafka, Pulsar) |
| Cenario | ID do cenário executado |
| Mensagens | Total de mensagens enviadas no cenário |
| TamanhoBytes | Tamanho do payload de cada mensagem |
| Producers | Número de producers concorrentes |
| Consumers | Número de consumers concorrentes |
| TaxaMsgSeg | Taxa alvo de mensagens/segundo (0 = máxima) |
| TempoSegundos | Tempo total de execução (produção + consumo) |
| ThroughputMsgSeg | Throughput real alcançado (msg/s) |
| LatenciaMediaMs | Latência média fim-a-fim |
| LatenciaMinMs | Latência mínima observada |
| LatenciaMaxMs | Latência máxima observada |
| LatenciaP95Ms | Percentil 95 da latência |
| CpuMediaPct | Uso médio de CPU do processo runner (%) |
| CpuMaxPct | Pico de uso de CPU (%) |
| MemoriaMediaMB | Memória média do processo runner (MB) |
| MemoriaMaxMB | Pico de memória do processo runner (MB) |

---

## Como Cada Métrica é Coletada

### 1. Tempo Total (TempoSegundos)

**Local:** `Program.cs` linhas 105-111

```csharp
var stopwatch = Stopwatch.StartNew();
await ProducerRunner.RunAsync(scenario, broker);
var consumerResult = await consumptionTask;
stopwatch.Stop();
double totalSeconds = stopwatch.Elapsed.TotalSeconds;
```

- **Início:** Logo antes de iniciar os producers
- **Fim:** Após todos os consumers confirmarem o recebimento de todas as mensagens esperadas
- **O que mede:** Tempo de "ponta a ponta" do pipeline completo (produção + entrega + consumo)

### 2. Throughput (ThroughputMsgSeg)

**Local:** `Program.cs` linha 119-120

```csharp
double throughput = scenario.MessageCount / totalSeconds;
```

- **Fórmula:** `Total de Mensagens / Tempo Total (segundos)`
- **Unidade:** mensagens por segundo (msg/s)
- **O que mede:** Capacidade de vazão do sistema sob a carga do cenário

### 3. Latências (Media, Min, Max, P95)

**Coleta:** `ConsumerRunner.cs` linhas 40-52

```csharp
consumer.StartAsync(
    message =>
    {
        double latencyMs = (DateTime.UtcNow - message.Timestamp).TotalMilliseconds;
        latenciesMs.Enqueue(latencyMs);
        // ...
    },
    cts.Token
);
```

**Cálculo:** `Program.cs` linhas 121-122 e `ComputeLatencyStats` (linhas 172-191)

```csharp
var (avgLatency, minLatency, maxLatency, p95Latency) =
    ComputeLatencyStats(consumerResult.LatenciesMs);
```

- **Timestamp da mensagem:** Definido no `ProducerRunner.cs` linha 73: `Timestamp = DateTime.UtcNow`
- **Medição no consumer:** `DateTime.UtcNow - message.Timestamp` no momento do callback `onMessageReceived`
- **P95:** Ordena as latências e pega o índice `Math.Ceiling(n * 0.95) - 1`
- **O que mede:** Tempo decorrido entre a criação da mensagem pelo producer e seu processamento pelo consumer

### 4. Uso de CPU (CpuMediaPct, CpuMaxPct)

**Local:** `ResourceMonitor.cs` linhas 55-92

```csharp
var currentCpuTime = _process.TotalProcessorTime;
double cpuPercent = 
    (currentCpuTime - _lastCpuTime).TotalMilliseconds
    / (now - _lastSampleAt).TotalMilliseconds
    * 100.0;
cpuPercent /= Environment.ProcessorCount;
```

- **Amostragem:** A cada 250ms durante toda a execução do benchmark
- **Método:** Diferença de `Process.TotalProcessorTime` entre amostras normalizada pelo tempo real e nº de cores
- **Escopo:** Apenas o **processo do runner** (benchmark), não dos brokers
- **O que mede:** Overhead computacional do cliente (serialização, rede, callbacks)

### 5. Uso de Memória (MemoriaMediaMB, MemoriaMaxMB)

**Local:** `ResourceMonitor.cs` linha 84

```csharp
double memoryMb = _process.WorkingSet64 / (1024.0 * 1024.0);
```

- **Amostragem:** A cada 250ms (mesmo loop da CPU)
- **Métrica:** `WorkingSet64` = memória física residente do processo
- **Escopo:** Apenas o **processo do runner**
- **O que mede:** Pressão de memória do cliente (buffers, objetos Message, filas concorrentes)

---

## Por Que Estas Medições Se Encaixam nos Três Pilares

### Pilar 1: Throughput (Vazão)

| Métrica | Papel no Pilar |
|---------|----------------|
| **ThroughputMsgSeg** | **Métrica principal** - quantifica mensagens processadas por unidade de tempo |
| **TempoSegundos** | Denominador do throughput; permite normalizar resultados |
| **Mensagens / TamanhoBytes / Producers / Consumers** | Contexto da carga aplicada; permite comparar "maçãs com maçãs" |
| **TaxaMsgSeg** | Diferencia cenários de "carga máxima" vs "carga controlada" |

**Por que funciona:** O throughput é a definição clássica de capacidade de um sistema de mensageria. Medir o tempo total do pipeline (produção + consumo de todas as mensagens) reflete a velocidade real que o sistema entrega valor ao consumidor final.

---

### Pilar 2: Latência (Tempo de Resposta)

| Métrica | Papel no Pilar |
|---------|----------------|
| **LatenciaMediaMs** | Latência típica esperada pelo usuário |
| **LatenciaMinMs** | Melhor caso |
| **LatenciaMaxMs** | Pior caso |
| **LatenciaP95Ms** | 95% das mensagens ficam abaixo deste valor, útil para mostrar nível do serviço |

**Por que funciona:** Latência em mensageria é **fim-a-fim** (producer → broker → consumer). A medição no callback do consumer captura exatamente isso:
- Inclui: serialização, envio de rede, processamento do broker, entrega, deserialização
- Não inclui: tempo de espera na fila do producer (rate limiting) - pois o timestamp é marcado **no momento do `PublishAsync`**
- P95 é padrão da indústria para SLAs (ex: "95% das mensagens < 50ms")

---

### Pilar 3: Uso de Recursos (Eficiência Operacional)

| Métrica | Papel no Pilar |
|---------|----------------|
| **CpuMediaPct / CpuMaxPct** | Custo computacional do cliente; identifica serialização cara, polling ineficiente, locks |
| **MemoriaMediaMB / MemoriaMaxMB** | Pressão de GC; identifica vazamentos, buffers excessivos, backpressure não gerenciado |

**Por que funciona:** 
- **Medir no cliente (runner)** e não no broker isola o custo da **biblioteca/cliente** - o que desenvolvedores podem otimizar
- CPU alta no runner = overhead de serialização (JSON), callbacks, context switching
- Memória crescente = buffers não liberados, `ConcurrentQueue` acumulando latências, GC pressure
- Correlação com throughput/latência revela trade-offs: ex: "Kafka tem maior throughput mas usa 2x mais CPU no cliente"

---