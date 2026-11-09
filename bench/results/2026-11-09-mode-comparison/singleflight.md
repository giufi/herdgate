# Mode comparison, singleflight

- Commit: 8dcb813
- Versions: golang:1.27.3 and redis:8.8.4-alpine images, Docker Engine 29.9.2 (Docker Desktop 4.98.0)
- Environment: Docker Desktop Linux VM (8 vCPUs, 7.6 GiB memory) on a laptop
- Command: `docker compose -f bench/compose.yaml run --rm bench -mode singleflight -trials 300 -csv /results/2026-11-09-mode-comparison/singleflight.csv`
- Per-trial rows: [singleflight.csv](singleflight.csv)

20 instances, 100 callers each, upstream latency seed 1
upstream: latency median 400ms, sigma 0.5, max 3s, concurrency limit 10, 100ms slot wait

| mode | trials | median requests per trial | max requests per trial | duplicate requests (total) | trials with 1 / 2 to 20 / over 20 requests | rejected requests | store hits | failed callers |
|---|---|---|---|---|---|---|---|---|
| singleflight | 300 | 20 | 29 | 5773 | 0 / 282 / 18 | 2997 | 41 | 294008 of 600000 |
