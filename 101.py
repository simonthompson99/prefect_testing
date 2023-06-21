import httpx
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact
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


@task
def tell_me_its_crap(loc: str):
    report = f"""# Weather is crap in:
{loc} :umbrella::cloud:"""
    create_markdown_artifact(
        key="crap weather report",
        markdown=report,
        description="where is the weather crap?"
    )


@flow(name="get some weather")
def get_weather(loc: str, lat: float, lon: float):
    LOGGER = get_run_logger()
    LOGGER.info(f"Fetching weather for {loc}")
    rain = fetch_rain(lat, lon)
    cloud = fetch_cloud(lat, lon)
    if rain > 1 or cloud > 90:
        tell_me_its_crap(loc)

@flow(name="getting all the weather")
def run_pipe(locations_dict: dict = LOCATIONS):
    for k, v in locations_dict.items():
        get_weather(k, v[0], v[1])

if __name__ == "__main__":
    run_pipe()
