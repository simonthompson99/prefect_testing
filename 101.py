import httpx
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
import csv
import datetime

LOCATIONS = {
    "sheffield": (53.4, -1.47),
    "paris": (48.9, 2.35),
    "london": (51.5, -0.12),
}

@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(minutes=1))
def fetch_rain(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="rain"),
    )
    most_recent_rain = float(weather.json()["hourly"]["rain"][0])
    return most_recent_rain

@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(minutes=1))
def fetch_cloud(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="cloudcover"),
    )
    most_recent_cloudcover = float(weather.json()["hourly"]["cloudcover"][0])
    return most_recent_cloudcover

@task(log_prints=True)
def tell_me_its_crap(loc: str):
    with open("weather.csv", "w+") as w:
        writer = csv.writer(w)
        writer.writerow([loc, datetime.datetime.now()])
    print(f"it's crap weather in {loc}")

@flow
def pipeline(loc: str, lat: float, lon: float):
    LOGGER = get_run_logger()
    LOGGER.info(f"Fetching weather for {loc}")
    rain = fetch_rain(lat, lon)
    cloud = fetch_cloud(lat, lon)
    if rain > 1 or cloud > 90:
        tell_me_its_crap(loc)


if __name__ == "__main__":
    for k, v in LOCATIONS.items():
        pipeline(k, v[0], v[1])
