#!/bin/bash
# Validate monitoring stack is operational

set -e

echo "Validating monitoring stack..."
echo ""

# Check Prometheus
echo "Checking Prometheus..."
if curl -sf http://localhost:9090/-/healthy > /dev/null; then
    echo "✓ Prometheus is healthy"
else
    echo "✗ Prometheus is not responding"
    exit 1
fi

# Check Grafana
echo "Checking Grafana..."
if curl -sf http://localhost:3000/api/health > /dev/null; then
    echo "✓ Grafana is healthy"
else
    echo "✗ Grafana is not responding"
    exit 1
fi

# Check Loki
echo "Checking Loki..."
if curl -sf http://localhost:3100/ready > /dev/null; then
    echo "✓ Loki is ready"
else
    echo "✗ Loki is not responding"
    exit 1
fi

# Check Prometheus targets
echo ""
echo "Checking Prometheus targets..."
TARGETS=$(curl -s http://localhost:9090/api/v1/targets | python3 -c "
import sys, json
data = json.load(sys.stdin)
active_targets = data.get('data', {}).get('activeTargets', [])
print(f'Active targets: {len(active_targets)}')
for target in active_targets:
    health = target.get('health', 'unknown')
    labels = target.get('labels', {})
    job = labels.get('job', 'unknown')
    print(f'  - {job}: {health}')
")

echo "$TARGETS"

echo ""
echo "✓ Monitoring stack validation complete!"
