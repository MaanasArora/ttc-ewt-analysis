from pathlib import Path
from google.transit import gtfs_realtime_pb2
import pandas as pd
from tqdm import tqdm


def parse_gtfs(filename):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(filename.read_bytes())
    return feed


def get_gtfs_records(files):
    records = []
    for file in tqdm(files):
        feed = parse_gtfs(file)
        timestamp = feed.header.timestamp
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                v = entity.vehicle
                record = {
                    "timestamp": timestamp,
                    "id": v.vehicle.id,
                    "vehicle_id": v.vehicle.id,
                    "route_id": v.trip.route_id,
                    "trip_id": v.trip.trip_id,
                    "direction_id": v.trip.direction_id,
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "speed": v.position.speed,
                    "stop_id": v.stop_id,
                    "current_status": v.current_status,
                }
                records.append(record)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("America/Toronto")
    return df


GROUP_COLS = ["stop_id", "route_id", "direction_id"]


def get_grouped_stopped_records(df: pd.DataFrame):
    stopped = df[
        df["current_status"] == gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
    ].copy()
    stopped["stop_id"] = stopped["stop_id"].astype(int)
    stopped = stopped.sort_values("timestamp").drop_duplicates(
        subset=["vehicle_id", "stop_id", "route_id"]
    )
    return stopped.groupby(GROUP_COLS)


def calculate_ewt(grouped: pd.core.groupby.DataFrameGroupBy) -> pd.DataFrame:
    """Compute EWT, headway stats, and record counts per stop/route/direction."""
    df = grouped.obj  # unwrap to the underlying filtered DataFrame

    headways = (
        df.sort_values(GROUP_COLS + ["timestamp"])
        .assign(
            headway=lambda x: (
                x.groupby(GROUP_COLS)["timestamp"].diff().dt.total_seconds().div(60)
            )
        )
        .dropna(subset=["headway"])
    )

    stats = headways.groupby(GROUP_COLS)["headway"].agg(
        headway_mean="mean",
        headway_var="var",
        headway_count="count",
    )

    stats["ewt"] = stats["headway_var"] / (
        2
        * stats["headway_mean"]
        .where(stats["headway_mean"] > 0)
        .where(stats["headway_count"] >= 8)
    )

    # Record counts from the original (pre-headway-diff) stopped records
    record_counts = grouped.size().rename("record_count")

    return stats.join(record_counts).reset_index()[
        GROUP_COLS
        + ["ewt", "headway_mean", "headway_var", "headway_count", "record_count"]
    ]


def main():
    files = list(Path("../ttc_data").glob("vehicle_*.pb"))

    min_time = min(f.stat().st_mtime for f in files)
    max_time = max(f.stat().st_mtime for f in files)
    duration = (max_time - min_time) / 60

    print(f"Found {len(files)} files created over a duration of {duration:.2f} minutes")

    df = get_gtfs_records(files)
    grouped_stop_route = get_grouped_stopped_records(df)

    ewt = (
        calculate_ewt(grouped_stop_route)
        .sort_values("ewt", ascending=False)
        .dropna(subset=["ewt"])
    )

    stops_path = Path("../static_data/stops.txt")
    if stops_path.exists():
        stops = pd.read_csv(stops_path)
        stops["stop_id"] = stops["stop_id"].astype(int)
        ewt = ewt.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")

    ewt.to_csv("ewt_by_stop_route.csv", index=False)
    print("Analysis complete. EWT by stop and route saved to ewt_by_stop_route.csv")


if __name__ == "__main__":
    main()
