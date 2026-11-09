# Instance scaling, singleflight, 5 instances

- Commit: 8dcb813
- Versions: golang:1.27.3 and redis:8.8.4-alpine images, Docker Engine 29.9.2 (Docker Desktop 4.98.0)
- Environment: Docker Desktop Linux VM (8 vCPUs, 7.6 GiB memory) on a laptop
- Command: `docker compose -f bench/compose.yaml run --rm bench -mode singleflight -instances 5 -trials 300 -csv /results/2026-11-09-instance-scaling/singleflight-5-instances.csv`
- Per-trial rows: [singleflight-5-instances.csv](singleflight-5-instances.csv)

5 instances, 100 callers each, upstream latency seed 1
upstream: latency median 400ms, sigma 0.5, max 3s, concurrency limit 10, 100ms slot wait

| mode | trials | median requests per trial | max requests per trial | duplicate requests (total) | trials with 1 / 2 to 5 / over 5 requests | rejected requests | store hits | failed callers |
|---|---|---|---|---|---|---|---|---|
| singleflight | 300 | 5 | 5 | 1200 | 0 / 300 / 0 | 0 | 0 | 0 of 150000 |
