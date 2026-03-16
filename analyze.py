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


def get_grouped_stopped_records(df):
    stopped = df[df["current_status"] == gtfs_realtime_pb2.VehiclePosition.STOPPED_AT]
    stopped["stop_id"] = stopped["stop_id"].astype(int)
    stopped = stopped.sort_values("timestamp")
    stopped = stopped.drop_duplicates(subset=["vehicle_id", "stop_id", "route_id"])
    stopped["route_id"].value_counts()
    grouped_stop_route = stopped.groupby(["stop_id", "route_id", "direction_id"])
    return grouped_stop_route


def calculate_headways(group):
    group = group.sort_values("timestamp").copy()
    group["headway"] = group["timestamp"].diff().dt.total_seconds() / 60
    return group.dropna(subset=["headway"])


def calculate_ewt(group):
    headways = calculate_headways(group)

    avg_wt = headways["headway"].mean()
    var_wt = headways["headway"].var()

    ewt = var_wt / (2 * avg_wt) if avg_wt > 0 else None
    return ewt


def main():
    files = list(Path("../ttc_data").glob("vehicle_*.pb"))

    min_time = min(f.stat().st_mtime for f in files)
    max_time = max(f.stat().st_mtime for f in files)
    duration = (max_time - min_time) / 60  # convert to minutes

    print(f"Found {len(files)} files created over a duration of {duration:.2f} minutes")

    df = get_gtfs_records(files)
    grouped_stop_route = get_grouped_stopped_records(df)
    ewt = grouped_stop_route[["timestamp"]].apply(calculate_ewt).reset_index(name="ewt")
    ewt = ewt.sort_values("ewt", ascending=False).dropna(subset=["ewt"])

    stops_path = Path("../static_data/stops.txt")
    if stops_path.exists():
        stops = pd.read_csv("../static_data/stops.txt")
        stops["stop_id"] = stops["stop_id"].astype(int)
        ewt = ewt.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")
        
    ewt.to_csv("ewt_by_stop_route.csv", index=False)

    print("Analysis complete. EWT by stop and route saved to ewt_by_stop_route.csv")


if __name__ == "__main__":
    main()
