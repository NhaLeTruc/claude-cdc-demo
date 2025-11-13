# E2E Tests Quick Start 🚀

## TL;DR - Just Run This

```bash
# Start everything (one command)
./scripts/e2e/start_test_infrastructure.sh

# Run E2E tests
poetry run pytest tests/e2e/test_postgres_to_delta.py -v

# Stop when done
./scripts/e2e/stop_test_infrastructure.sh
```

## Alternative: Using Docker Only

```bash
# Start all services in Docker (includes streaming pipeline)
docker compose -f compose/docker-compose.yml up -d spark-delta-streaming

# Run tests
poetry run pytest tests/e2e/test_postgres_to_delta.py -v
```

## Alternative: Automatic (Zero Setup)

```bash
# Tests automatically start/stop infrastructure
poetry run pytest tests/e2e/test_postgres_to_delta.py -v
```

## Check Health Before Testing

```bash
./scripts/e2e/health_check.sh
```

## What Was Fixed

The E2E tests were skipping because:
- ❌ Missing streaming pipeline configuration in `.env`
- ❌ No Kafka → Delta Lake streaming job running
- ❌ Delta table didn't exist

Now:
- ✅ Configuration added to `.env`
- ✅ Helper scripts to start/stop infrastructure
- ✅ Docker service for containerized streaming
- ✅ Automatic pytest fixtures
- ✅ Health check utilities
- ✅ Complete documentation

## Files You Need to Know

| File | Purpose |
|------|---------|
| `scripts/e2e/start_test_infrastructure.sh` | Start everything |
| `scripts/e2e/stop_test_infrastructure.sh` | Stop infrastructure |
| `scripts/e2e/health_check.sh` | Validate services |
| `docs/E2E_TESTING.md` | Complete guide |
| `docs/E2E_TESTS_SETUP_SUMMARY.md` | Implementation details |
| `tests/e2e/conftest.py` | Automatic fixtures |

## Troubleshooting

**Tests still skipping?**
```bash
./scripts/e2e/health_check.sh  # Shows what's wrong
```

**Need to restart streaming pipeline?**
```bash
./scripts/pipelines/orchestrate_streaming_pipelines.sh restart delta
```

**Check logs:**
```bash
tail -f /tmp/kafka-to-delta.log
```

## Read More

- [E2E_TESTING.md](docs/E2E_TESTING.md) - Full testing guide
- [E2E_TESTS_SETUP_SUMMARY.md](docs/E2E_TESTS_SETUP_SUMMARY.md) - What was implemented
- [STREAMING_PIPELINES.md](docs/STREAMING_PIPELINES.md) - Pipeline architecture