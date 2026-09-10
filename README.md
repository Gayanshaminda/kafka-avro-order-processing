# Kafka Order Processing Assignment

This project implements the complete Chapter 3 assignment: Avro-encoded order
messages are produced to Kafka, consumed in real time, included in running price
averages, retried after temporary failures, and routed to a Dead Letter Queue
(DLQ) after permanent failure or exhausted retries.

## Requirement coverage

| Assignment requirement | Implementation |
| --- | --- |
| Kafka producer and consumer | `src/producer.py` and `src/consumer.py` |
| Avro serialization | `schemas/order.avsc` and `src/avro_codec.py` |
| Running average | Overall and per-product averages in `src/processing.py` |
| Temporary-failure retry | `orders.retry`, retry count headers, exponential backoff |
| Permanent failure handling | `orders.dlq` plus `src/dlq_consumer.py` |
| Live demonstration | One-command deterministic demo in `compose.yaml` |
| Git submission | Repository-ready source, tests, ignore rules, and documentation |

## Architecture

```text
Producer --Avro--> orders ----success----> running averages
                         |
                         +--temporary----> orders.retry --reprocess--+
                         |                                           |
                         +--permanent---------------------------> orders.dlq
                                              retry exhausted ---> orders.dlq
                                                                    |
                                                               DLQ monitor
```

The three Kafka topics have one partition and one replica for a lightweight
local demonstration. The consumer uses manual offset commits: it commits only
after successful aggregation or confirmed forwarding to the retry/DLQ topic.
The forwarding producer has Kafka idempotence enabled.

## Run the live demo

Prerequisite: Docker Desktop must be installed and running.

```powershell
docker compose up --build
```

The producer sends six deterministic Avro orders:

- Orders `1001`-`1004` succeed immediately.
- Order `1005` fails temporarily twice, enters `orders.retry`, then succeeds.
- Order `1006` fails permanently and appears in `orders.dlq`.

Watch for these log labels:

- `PRODUCED`: producer sent an Avro order.
- `AGGREGATED`: price was added to the live averages.
- `RETRY`: temporary failure was forwarded to `orders.retry`.
- `DLQ permanent failure`: consumer routed a record to `orders.dlq`.
- `DLQ_MESSAGE`: DLQ monitor read the failed Avro order and its error metadata.

After the temporary order succeeds, the expected successful prices are 10, 20,
30, 40, and 50. Therefore, the final overall count is `5` and the final running
average is `30.00`. The permanently failed price 60 is not aggregated.

Stop and remove the demo containers with:

```powershell
docker compose down
```

To also erase locally stored Kafka data and repeat the demo from a completely
clean state:

```powershell
docker compose down -v
```

## Run without Dockerized applications

Start only Kafka:

```powershell
docker compose up -d kafka
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

Then use three terminals:

```powershell
python -m src.consumer
python -m src.dlq_consumer
python -m src.producer --demo
```

Use `python -m src.producer --count 20` to produce random valid orders.

## Tests

```powershell
pytest -q
```

Or run the tests entirely in Docker, without installing Python packages locally:

```powershell
docker build --target test -t kafka-order-assignment-tests .
```

The tests verify Avro binary round-tripping, overall and per-product averages,
duplicate suppression, validation failures, and deterministic retry behavior.

## How retry and DLQ handling work

Kafka headers carry metadata while the message value stays compliant with the
required `Order` Avro schema. Important headers include `retry-attempt`,
`failure-mode`, `last-error`, and the original topic/partition/offset.

On a temporary error, the consumer increases `retry-attempt`, waits using
exponential backoff, publishes the same Avro bytes to `orders.retry`, confirms
delivery, and then commits the source offset. After `MAX_RETRIES`, it publishes
to `orders.dlq`. A permanent error skips retries and goes directly to the DLQ.

The live demo uses a header only to trigger predictable failures. In a real
system, the same exception paths would wrap calls to a database, payment API, or
inventory service.

## Reliability notes

- Kafka delivery is at-least-once; the consumer's in-memory order-ID set avoids
  double-counting duplicate deliveries during the current run.
- Running averages reset when the consumer restarts. Production systems would
  persist state in a database or use Kafka Streams/state stores.
- This local broker uses plaintext networking and one replica for demonstration,
  not production deployment.

## Suggested live explanation

1. Show `order.avsc` and explain the three required fields.
2. Start the Compose demo and point out each `PRODUCED` line.
3. Show `AGGREGATED` lines changing the running average.
4. Point out two `RETRY` lines for order `1005`, followed by its success.
5. Show the permanent failure for `1006` and the `DLQ_MESSAGE` line.
6. Open the tests and explain how the core behavior is verified independently.

## Git submission

Make meaningful commits and push the repository URL requested by the lecturer:

```powershell
git init
git add .
git commit -m "Implement Kafka Avro order processing assignment"
git branch -M main
git remote add origin YOUR_REPOSITORY_URL
git push -u origin main
```

Do not commit generated virtual environments, logs, or Kafka data; `.gitignore`
already excludes them.
