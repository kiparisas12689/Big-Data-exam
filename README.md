# Big Data Examination: Detection of Vessel Collisions

This repository contains a Dockerized PySpark solution for detecting the closest moving-vessel encounter in Danish AIS data for December 2021.

The pipeline:

- reads Danish AIS CSV files with Apache Spark;
- filters the exact period from `2021-12-01 00:00:00` to `2021-12-31 23:59:59`;
- keeps vessels inside a 50 nautical mile radius around `55.225000, 14.245000`;
- removes stationary vessels and common AIS/GPS noise;
- detects close encounters using spatial grid keys and time buckets instead of a full Cartesian product;
- outputs the collided or closest vessel pair, event coordinates, candidate table, and a 20-minute trajectory visualization.

## Project Structure

```text
.
├── Data/                         # Local AIS CSV files mounted into Docker
├── src/collision_detection.py    # PySpark collision detection pipeline
├── outputs/                      # Generated at runtime
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── REPORT.md
└── README.md
```

## Build

```bash
docker compose build
```

The `Data/` directory is intentionally excluded from the Docker build context by `.dockerignore`, because the raw AIS files are large. At runtime it is mounted read-only as `/data`.

## Run

```bash
docker compose run --rm collision-detector
```

Expected outputs in `outputs/`:

- `collision_result.json` - final MMSI numbers, vessel names, timestamp, coordinates, separation distance, and run parameters.
- `collision_candidates.csv` - top closest candidate pairs after Spark filtering.
- `collision_trajectories_20min.csv` - AIS points for both vessels from 10 minutes before to 10 minutes after the closest approach.
- `collision_trajectories.png` - plotted trajectory map for the same 20-minute window.

## Optional Parameters

You can override the defaults through Compose:

```bash
docker compose run --rm collision-detector \
  --max-candidate-distance-nm 0.15 \
  --max-time-delta-seconds 30 \
  --shuffle-partitions 240
```

Important defaults:

- study radius: `50` nautical miles;
- moving vessel threshold: `SOG >= 1.0` knot;
- maximum accepted AIS SOG: `55` knots;
- maximum implied jump speed: `65` knots;
- time join tolerance: `60` seconds;
- grid size: `0.02` degrees;
- close-candidate radius: `0.25` nautical miles;
- physical-collision flag: `distance <= 0.05` nautical miles.

## Local Non-Docker Run

If Python, Java, and Spark-compatible dependencies are installed locally:

```bash
pip install -r requirements.txt
spark-submit src/collision_detection.py --input "Data/aisdk-2021-12-*.csv" --output-dir outputs
```

Docker is the recommended reproducible path for grading.

## Method Summary

The solution avoids an unbounded pairwise comparison. Spark first reduces the raw AIS data to valid, moving points in the assignment area. It then creates one-minute time buckets and approximately 0.02-degree spatial grid cells. Each point is expanded only to adjacent time and spatial cells, and the self-join is performed on those keys with `mmsi_a < mmsi_b`. The expensive haversine distance is calculated only for these local candidates.

See `REPORT.md` for details on cleaning, noise filtering, and collision verification.

## Result From the Included December Run

The completed full-data run found:

- MMSI `219017554`, vessel `SILLE BOB`;
- MMSI `219021219`, vessel `JANNE`;
- closest approach at `2021-12-29 13:44:42`;
- estimated coordinates `55.1886355, 14.701795`;
- separation `0.00171165` nautical miles, approximately `3.17` meters.

This is flagged as a likely physical collision because it is below the configured `0.05` nautical mile threshold.
