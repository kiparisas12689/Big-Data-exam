import argparse
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


EARTH_RADIUS_NM = 3440.065
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
DEFAULT_START = "2021-12-01 00:00:00"
DEFAULT_END = "2022-01-01 00:00:00"


AIS_SCHEMA = T.StructType(
    [
        T.StructField("# Timestamp", T.StringType(), True),
        T.StructField("Type of mobile", T.StringType(), True),
        T.StructField("MMSI", T.StringType(), True),
        T.StructField("Latitude", T.StringType(), True),
        T.StructField("Longitude", T.StringType(), True),
        T.StructField("Navigational status", T.StringType(), True),
        T.StructField("ROT", T.StringType(), True),
        T.StructField("SOG", T.StringType(), True),
        T.StructField("COG", T.StringType(), True),
        T.StructField("Heading", T.StringType(), True),
        T.StructField("IMO", T.StringType(), True),
        T.StructField("Callsign", T.StringType(), True),
        T.StructField("Name", T.StringType(), True),
        T.StructField("Ship type", T.StringType(), True),
        T.StructField("Cargo type", T.StringType(), True),
        T.StructField("Width", T.StringType(), True),
        T.StructField("Length", T.StringType(), True),
        T.StructField("Type of position fixing device", T.StringType(), True),
        T.StructField("Draught", T.StringType(), True),
        T.StructField("Destination", T.StringType(), True),
        T.StructField("ETA", T.StringType(), True),
        T.StructField("Data source type", T.StringType(), True),
        T.StructField("A", T.StringType(), True),
        T.StructField("B", T.StringType(), True),
        T.StructField("C", T.StringType(), True),
        T.StructField("D", T.StringType(), True),
    ]
)


def haversine_nm(lat1, lon1, lat2, lon2):
    """Spark expression for great-circle distance in nautical miles."""
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lam = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(d_phi / 2.0), 2)
        + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(d_lam / 2.0), 2)
    )
    return F.lit(EARTH_RADIUS_NM) * (F.lit(2.0) * F.asin(F.sqrt(a)))


def read_and_clean_ais(spark, args):
    """Load December AIS rows, keep moving vessels in the study area, and remove noisy jumps."""
    lat_delta = args.radius_nm / 60.0
    lon_delta = args.radius_nm / (60.0 * math.cos(math.radians(CENTER_LAT)))

    raw = spark.read.option("header", True).schema(AIS_SCHEMA).csv(args.input)

    base = (
        raw.select(
            F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("event_time"),
            F.col("Type of mobile").alias("mobile_type"),
            F.col("MMSI").cast("long").alias("mmsi"),
            F.col("Latitude").cast("double").alias("lat"),
            F.col("Longitude").cast("double").alias("lon"),
            F.lower(F.coalesce(F.col("Navigational status"), F.lit(""))).alias("nav_status"),
            F.col("SOG").cast("double").alias("sog"),
            F.col("COG").cast("double").alias("cog"),
            F.trim(F.coalesce(F.col("Name"), F.lit(""))).alias("name"),
            F.col("Ship type").alias("ship_type"),
            F.col("Length").cast("double").alias("length_m"),
            F.col("Width").cast("double").alias("width_m"),
        )
        .where(F.col("event_time") >= F.to_timestamp(F.lit(args.start)))
        .where(F.col("event_time") < F.to_timestamp(F.lit(args.end)))
        .where(F.col("mmsi").between(100000000, 999999999))
        .where(F.col("lat").between(-90.0, 90.0) & F.col("lon").between(-180.0, 180.0))
        .where(F.col("lat").between(CENTER_LAT - lat_delta, CENTER_LAT + lat_delta))
        .where(F.col("lon").between(CENTER_LON - lon_delta, CENTER_LON + lon_delta))
        .withColumn("distance_to_center_nm", haversine_nm(F.col("lat"), F.col("lon"), F.lit(CENTER_LAT), F.lit(CENTER_LON)))
        .where(F.col("distance_to_center_nm") <= args.radius_nm)
        .where(F.col("sog").isNotNull())
        .where((F.col("sog") >= args.min_sog_knots) & (F.col("sog") <= args.max_sog_knots))
        .where(~F.col("nav_status").rlike("anchor|moored|aground|not defined|reserved"))
        .repartition(args.shuffle_partitions, "mmsi")
    )

    w = Window.partitionBy("mmsi").orderBy("event_time")
    with_prev = (
        base.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_time", F.lag("event_time").over(w))
        .withColumn("dt_hours", (F.col("event_time").cast("long") - F.col("prev_time").cast("long")) / 3600.0)
        .withColumn(
            "segment_nm",
            F.when(F.col("prev_lat").isNotNull(), haversine_nm(F.col("lat"), F.col("lon"), F.col("prev_lat"), F.col("prev_lon"))),
        )
        .withColumn("observed_speed_knots", F.col("segment_nm") / F.col("dt_hours"))
    )

    cleaned = (
        with_prev.where(
            F.col("prev_lat").isNull()
            | (F.col("dt_hours") <= 0)
            | (
                (F.col("dt_hours") >= args.min_jump_dt_seconds / 3600.0)
                & (F.col("observed_speed_knots") <= args.max_jump_speed_knots)
            )
        )
        .drop("prev_lat", "prev_lon", "prev_time")
        .persist()
    )
    return cleaned


def add_base_keys(df, args):
    """Add non-expanded time and space keys for the left side of the proximity join."""
    return (
        df.withColumn("time_bucket", F.floor(F.col("event_time").cast("long") / args.time_bucket_seconds))
        .withColumn("lat_cell", F.floor(F.col("lat") / args.grid_degrees).cast("long"))
        .withColumn("lon_cell", F.floor(F.col("lon") / args.grid_degrees).cast("long"))
    )


def add_neighbor_keys(df, args):
    """Expand only the right side into neighboring grid/time keys to avoid duplicate pair work."""
    return (
        add_base_keys(df, args)
        .withColumn("join_time_bucket", F.explode(F.sequence(F.col("time_bucket") - 1, F.col("time_bucket") + 1)))
        .withColumn("join_lat_cell", F.explode(F.sequence(F.col("lat_cell") - 1, F.col("lat_cell") + 1)))
        .withColumn("join_lon_cell", F.explode(F.sequence(F.col("lon_cell") - 1, F.col("lon_cell") + 1)))
    )


def find_collision_candidates(cleaned, args):
    left = add_base_keys(cleaned, args).select(
        "mmsi",
        "event_time",
        "lat",
        "lon",
        "sog",
        "cog",
        "name",
        "length_m",
        "width_m",
        "time_bucket",
        "lat_cell",
        "lon_cell",
    )

    right = add_neighbor_keys(cleaned, args).select(
        "mmsi",
        "event_time",
        "lat",
        "lon",
        "sog",
        "cog",
        "name",
        "length_m",
        "width_m",
        "join_time_bucket",
        "join_lat_cell",
        "join_lon_cell",
    )

    a = left.alias("a")
    b = right.alias("b")
    joined = a.join(
        b,
        on=[
            F.col("a.time_bucket") == F.col("b.join_time_bucket"),
            F.col("a.lat_cell") == F.col("b.join_lat_cell"),
            F.col("a.lon_cell") == F.col("b.join_lon_cell"),
            F.col("a.mmsi") < F.col("b.mmsi"),
        ],
        how="inner",
    )

    candidates = (
        joined.withColumn(
            "time_delta_seconds",
            F.abs(F.col("a.event_time").cast("long") - F.col("b.event_time").cast("long")),
        )
        .where(F.col("time_delta_seconds") <= args.max_time_delta_seconds)
        .withColumn("distance_nm", haversine_nm(F.col("a.lat"), F.col("a.lon"), F.col("b.lat"), F.col("b.lon")))
        .where(F.col("distance_nm") <= args.max_candidate_distance_nm)
        .withColumn(
            "collision_time",
            F.to_timestamp(
                F.from_unixtime(
                    ((F.col("a.event_time").cast("long") + F.col("b.event_time").cast("long")) / 2.0).cast("long")
                )
            ),
        )
        .withColumn("collision_lat", (F.col("a.lat") + F.col("b.lat")) / 2.0)
        .withColumn("collision_lon", (F.col("a.lon") + F.col("b.lon")) / 2.0)
        .select(
            F.col("a.mmsi").alias("mmsi_a"),
            F.col("b.mmsi").alias("mmsi_b"),
            F.col("a.name").alias("name_a"),
            F.col("b.name").alias("name_b"),
            F.col("a.event_time").alias("time_a"),
            F.col("b.event_time").alias("time_b"),
            "collision_time",
            F.col("a.lat").alias("lat_a"),
            F.col("a.lon").alias("lon_a"),
            F.col("b.lat").alias("lat_b"),
            F.col("b.lon").alias("lon_b"),
            "collision_lat",
            "collision_lon",
            F.col("a.sog").alias("sog_a"),
            F.col("b.sog").alias("sog_b"),
            F.col("a.cog").alias("cog_a"),
            F.col("b.cog").alias("cog_b"),
            F.col("a.length_m").alias("length_m_a"),
            F.col("b.length_m").alias("length_m_b"),
            "time_delta_seconds",
            "distance_nm",
        )
    )
    return candidates


def save_single_csv(df, path):
    tmp_path = f"{path}.spark_parts"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(tmp_path)
    )
    tmp = Path(tmp_path)
    part = next(tmp.glob("part-*.csv"))
    final_path = Path(path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists():
        final_path.unlink()
    part.replace(final_path)
    for child in tmp.iterdir():
        child.unlink()
    tmp.rmdir()


def vessel_names(cleaned, mmsi_values):
    names = (
        cleaned.where(F.col("mmsi").isin(mmsi_values))
        .where(F.length("name") > 0)
        .groupBy("mmsi", "name")
        .count()
        .orderBy(F.col("mmsi"), F.col("count").desc())
        .dropDuplicates(["mmsi"])
        .select("mmsi", "name")
        .collect()
    )
    return {row["mmsi"]: row["name"] for row in names}


def trajectory_window(cleaned, collision, args):
    mmsis = [collision["mmsi_a"], collision["mmsi_b"]]
    center_ts = collision["collision_time"]
    start_expr = F.to_timestamp(F.lit(center_ts)) - F.expr(f"INTERVAL {args.trajectory_minutes} MINUTES")
    end_expr = F.to_timestamp(F.lit(center_ts)) + F.expr(f"INTERVAL {args.trajectory_minutes} MINUTES")
    return (
        cleaned.where(F.col("mmsi").isin(mmsis))
        .where((F.col("event_time") >= start_expr) & (F.col("event_time") <= end_expr))
        .select("mmsi", "name", "event_time", "lat", "lon", "sog", "cog")
        .orderBy("mmsi", "event_time")
    )


def plot_trajectories(rows, collision, output_png):
    import matplotlib.pyplot as plt

    points_by_mmsi = {}
    for row in rows:
        points_by_mmsi.setdefault(row["mmsi"], []).append(row)

    fig, ax = plt.subplots(figsize=(10, 8))
    colors = ["#1f77b4", "#d62728"]
    for idx, (mmsi, points) in enumerate(points_by_mmsi.items()):
        points = sorted(points, key=lambda r: r["event_time"])
        label_name = points[0]["name"] or "Unknown"
        ax.plot(
            [p["lon"] for p in points],
            [p["lat"] for p in points],
            marker="o",
            markersize=2.5,
            linewidth=1.4,
            color=colors[idx % len(colors)],
            label=f"{mmsi} - {label_name}",
        )
        ax.scatter(points[0]["lon"], points[0]["lat"], s=60, marker="s", color=colors[idx % len(colors)])
        ax.scatter(points[-1]["lon"], points[-1]["lat"], s=70, marker="^", color=colors[idx % len(colors)])

    ax.scatter(
        [collision["collision_lon"]],
        [collision["collision_lat"]],
        s=130,
        marker="x",
        color="black",
        linewidths=2.2,
        label="Closest approach",
    )
    ax.set_title("Vessel trajectories: 10 minutes before and after closest approach")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best")
    ax.set_aspect("equal", adjustable="datalim")
    fig.tight_layout()
    Path(output_png).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_png, dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Detect closest moving-vessel collision candidate in Danish AIS data.")
    parser.add_argument("--input", default="/data/aisdk-2021-12-*.csv", help="CSV glob or directory mounted in Docker.")
    parser.add_argument("--output-dir", default="/outputs", help="Directory for JSON, CSV, and PNG outputs.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--radius-nm", type=float, default=50.0)
    parser.add_argument("--min-sog-knots", type=float, default=1.0)
    parser.add_argument("--max-sog-knots", type=float, default=55.0)
    parser.add_argument("--max-jump-speed-knots", type=float, default=65.0)
    parser.add_argument("--min-jump-dt-seconds", type=float, default=30.0)
    parser.add_argument("--time-bucket-seconds", type=int, default=60)
    parser.add_argument("--max-time-delta-seconds", type=int, default=60)
    parser.add_argument("--grid-degrees", type=float, default=0.02)
    parser.add_argument("--max-candidate-distance-nm", type=float, default=0.25)
    parser.add_argument("--physical-collision-distance-nm", type=float, default=0.05)
    parser.add_argument("--trajectory-minutes", type=int, default=10)
    parser.add_argument("--top-candidates", type=int, default=100)
    parser.add_argument("--shuffle-partitions", type=int, default=160)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("Danish AIS Vessel Collision Detection")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    cleaned = read_and_clean_ais(spark, args)
    cleaned_count = cleaned.count()
    print(f"Clean moving AIS points inside study area after noise filtering: {cleaned_count}")

    candidates = find_collision_candidates(cleaned, args).persist()
    top_candidates = candidates.orderBy(F.col("distance_nm").asc(), F.col("time_delta_seconds").asc()).limit(args.top_candidates)
    top_rows = top_candidates.collect()
    if not top_rows:
        raise RuntimeError(
            "No close moving-vessel candidates were found. Increase --max-candidate-distance-nm "
            "or inspect the cleaning thresholds."
        )

    top_candidates_df = spark.createDataFrame(top_rows, schema=top_candidates.schema)
    save_single_csv(top_candidates_df, output_dir / "collision_candidates.csv")
    collision_row = top_rows[0].asDict()
    names = vessel_names(cleaned, [collision_row["mmsi_a"], collision_row["mmsi_b"]])
    collision_row["name_a"] = collision_row["name_a"] or names.get(collision_row["mmsi_a"], "Unknown")
    collision_row["name_b"] = collision_row["name_b"] or names.get(collision_row["mmsi_b"], "Unknown")
    collision_row["likely_physical_collision"] = collision_row["distance_nm"] <= args.physical_collision_distance_nm
    collision_row["distance_meters"] = collision_row["distance_nm"] * 1852.0

    trajectory = trajectory_window(cleaned, collision_row, args).persist()
    save_single_csv(trajectory, output_dir / "collision_trajectories_20min.csv")
    trajectory_rows = trajectory.collect()
    plot_trajectories(trajectory_rows, collision_row, output_dir / "collision_trajectories.png")

    summary = {
        "mmsi_a": collision_row["mmsi_a"],
        "name_a": collision_row["name_a"],
        "mmsi_b": collision_row["mmsi_b"],
        "name_b": collision_row["name_b"],
        "collision_time": collision_row["collision_time"].isoformat(sep=" "),
        "collision_latitude": collision_row["collision_lat"],
        "collision_longitude": collision_row["collision_lon"],
        "distance_nm": collision_row["distance_nm"],
        "distance_meters": collision_row["distance_meters"],
        "time_delta_seconds": collision_row["time_delta_seconds"],
        "likely_physical_collision": collision_row["likely_physical_collision"],
        "clean_points_processed": cleaned_count,
        "candidate_pairs_reported": len(top_rows),
        "parameters": vars(args),
    }
    with open(output_dir / "collision_result.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)

    print("\nClosest moving-vessel encounter")
    print("--------------------------------")
    print(f"MMSI A: {summary['mmsi_a']} ({summary['name_a']})")
    print(f"MMSI B: {summary['mmsi_b']} ({summary['name_b']})")
    print(f"Time: {summary['collision_time']}")
    print(f"Coordinates: {summary['collision_latitude']:.6f}, {summary['collision_longitude']:.6f}")
    print(f"Separation: {summary['distance_nm']:.5f} nm / {summary['distance_meters']:.1f} m")
    print(f"Likely physical collision: {summary['likely_physical_collision']}")
    print(f"Outputs written to: {output_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
