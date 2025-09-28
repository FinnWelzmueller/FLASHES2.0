from fastapi import APIRouter
from fastapi import HTTPException
import pandas as pd
from backend.config import sources_collection, field_lookup, influx_bucket, influx_org, query_api

router = APIRouter()
@router.get("/sources")
def get_all_sources():
    sources = list(sources_collection.find({}))
    if not sources:
        raise HTTPException(status_code=404, detail="No sources found")

@router.get("/sources/{name}")
def get_source_by_name(name: str):
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source

@router.get("/sources/{name}/{telescope}")
def get_flux_by_name_and_telescope(name: str, telescope: str):
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    if not source.get(telescope):
        raise HTTPException(status_code=404, detail="Unsupported telescope for source" )

# endpoints for data
@router.get("/sources/{name}/{data}")
def get_flux_by_field(name: str, field: str):
    # MongoDB-Quelle abrufen
    source = sources_collection.find_one({"_id": name})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    # Field validieren
    if field not in field_lookup:
        raise HTTPException(status_code=400, detail="Unsupported field")

    # Influx-Key finden (prüfen alle Teleskope)
    key = None
    for telescope in ["maxi", "swift", "fermi"]:
        if telescope in source and source[telescope]:
            if "influx_key" in source[telescope]:
                key = source[telescope]["influx_key"]
                break
    if not key:
        raise HTTPException(status_code=404, detail="No influx key found for source")

    # InfluxDB-Abfrage
    influx_field = field_lookup[field]
    query = f"""
    from(bucket: "{influx_bucket}")
      |> range(start: 0)
      |> filter(fn: (r) => r._measurement == "flux data")
      |> filter(fn: (r) => r._field == "{influx_field}")
      |> filter(fn: (r) => r.source == "{key}")
      |> sort(columns: ["_time"])
    """
    tables = query_api.query(org=influx_org, query=query)

    times, values = [], []
    for table in tables:
        for record in table.records:
            times.append(record.get_time())
            values.append(record.get_value())

    df = pd.DataFrame({"time": times, "value": values})
    return df.to_dict(orient="records")