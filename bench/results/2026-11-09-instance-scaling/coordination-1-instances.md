# Instance scaling, coordination, 1 instances

- Commit: 8dcb813
- Versions: golang:1.27.3 and redis:8.8.4-alpine images, Docker Engine 29.9.2 (Docker Desktop 4.98.0)
- Environment: Docker Desktop Linux VM (8 vCPUs, 7.6 GiB memory) on a laptop
- Command: `docker compose -f bench/compose.yaml run --rm bench -mode coordination -instances 1 -trials 300 -csv /results/2026-11-09-instance-scaling/coordination-1-instances.csv`
- Per-trial rows: [coordination-1-instances.csv](coordination-1-instances.csv)

1 instances, 100 callers each, upstream latency seed 1
upstream: latency median 400ms, sigma 0.5, max 3s, concurrency limit 10, 100ms slot wait

| mode | trials | median requests per trial | max requests per trial | duplicate requests (total) | trials with 1 / 2 to 1 / over 1 requests | rejected requests | store hits | failed callers |
|---|---|---|---|---|---|---|---|---|
| coordination | 300 | 1 | 1 | 0 | 300 / 0 / 0 | 0 | 0 | 0 of 30000 |
