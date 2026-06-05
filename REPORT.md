# Written Report

## Objective

The goal is to identify the two moving vessels with the closest physical proximity inside the required Danish AIS study area during December 2021, then visualize both trajectories from exactly 10 minutes before to 10 minutes after the closest approach.

## Data and Study Area

The input data is the Danish AIS CSV dataset for `2021-12-01` through `2021-12-31`. The Spark job filters timestamps from `2021-12-01 00:00:00` inclusive to `2022-01-01 00:00:00` exclusive. Spatial filtering uses a two-step method:

1. a cheap latitude/longitude bounding box around the required center point;
2. an exact haversine distance filter retaining only points within 50 nautical miles of `55.225000, 14.245000`.

This avoids computing full great-circle distances for records that are obviously outside the study area.

## Cleaning and Noise Handling

AIS contains invalid coordinates, missing fields, stationary ships, duplicate-like transmissions, and occasional GPS jumps. The pipeline applies the following controls before collision detection:

- rejects invalid MMSI values and impossible coordinates;
- keeps only rows with parseable timestamps and numeric latitude, longitude, and SOG;
- removes rows where `SOG < 1.0` knot to exclude anchored, docked, or drifting stationary vessels;
- removes navigational statuses containing anchored, moored, aground, undefined, or reserved states;
- rejects extreme AIS speed values above 55 knots;
- uses a Spark window per MMSI to compare consecutive vessel positions and removes points implying movement above 65 knots, except where the time delta is too small to be meaningful.

These choices are conservative for commercial traffic near Bornholm and reduce the risk that one noisy GPS jump becomes a false collision.

## Collision Detection Method

The solution does not use an unoptimized Cartesian product. After cleaning, each AIS point receives:

- a one-minute time bucket;
- a latitude grid cell;
- a longitude grid cell.

Each point is expanded only into neighboring time and spatial cells. Spark then self-joins on those keys and keeps only pairs from different vessels (`mmsi_a < mmsi_b`) whose timestamps are within 60 seconds. Haversine distance is calculated only after this reduction. The closest candidate is selected by minimum separation distance, with time difference as a tie-breaker.

The final JSON result includes a `likely_physical_collision` flag. By default, the flag is true when separation is at most `0.05` nautical miles, approximately 92.6 meters. The broader candidate threshold is `0.25` nautical miles so the algorithm can still report the closest possible encounter if no physically overlapping AIS points exist.

## Computational Strategy

The highest-cost operation is comparing vessel positions. To control this cost, the pipeline:

- filters by time, geography, movement, and noise before any pair join;
- repartitions by MMSI for per-vessel window calculations;
- uses adaptive Spark execution and configurable shuffle partitions;
- uses grid/time bucketing so only nearby records can be joined;
- writes only the top candidate list and small 20-minute trajectory window to driver-side plotting code.

The visualization is generated after Spark has reduced the data to two vessels and a short time window, so standard plotting libraries are used only for a small output artifact, not for raw data processing.

## Outputs and Findings

The full Docker run over all December files completed successfully. After filtering and cleaning, Spark processed `674,062` moving AIS points inside the 50 nautical mile study area.

The closest moving-vessel encounter found was:

- Vessel A: `SILLE BOB`, MMSI `219017554`;
- Vessel B: `JANNE`, MMSI `219021219`;
- closest approach time: `2021-12-29 13:44:42`;
- estimated closest-approach coordinate: latitude `55.1886355`, longitude `14.701795`;
- separation: `0.00171165` nautical miles, approximately `3.17` meters;
- AIS timestamp delta between the two positions: `26` seconds;
- physical-collision flag: `true`.

The command used was:

```bash
docker compose run --rm collision-detector
```

The definitive result is written to:

- `outputs/collision_result.json`
- `outputs/collision_trajectories.png`
- `outputs/collision_trajectories_20min.csv`
- `outputs/collision_candidates.csv`

The JSON file explicitly states:

- MMSI and vessel name for both vessels;
- exact closest-approach timestamp;
- estimated collision latitude and longitude;
- separation in nautical miles and meters;
- whether it satisfies the physical-collision distance threshold.

## Verification

Verification consists of checking that:

- both vessels are moving before and after the event;
- their AIS points are temporally close, within the configured tolerance;
- the closest point is not produced by an impossible jump;
- the generated PNG shows the two trajectories approaching the same coordinate within the required 20-minute window.

If the top result is slightly above the physical threshold, the candidate is still the closest moving-vessel encounter found under the assignment constraints, and the result should be described as closest possible physical proximity rather than a confirmed hull collision.
