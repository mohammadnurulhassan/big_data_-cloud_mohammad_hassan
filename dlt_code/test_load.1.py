# dlt_code/load_job_ads.py
import dlt
import requests
import json
from typing import Dict, Iterator, Optional, List

dlt.config["load.truncate_staging_dataset"] = True

# Define the occupation fields you want to pull
OCCUPATION_FIELDS = ["yhCP_AqT_tns", "Uuf1_GMh_Uvw", "9puE_nYg_crq"]


def _get_ads(url_for_search: str, p: Dict) -> Dict:
    headers = {"accept": "application/json"}
    resp = requests.get(url_for_search, headers=headers, params=p, timeout=30)
    resp.raise_for_status()
    return resp.json()


@dlt.resource(table_name="job_ads", write_disposition="merge", primary_key="id")
def jobsearch_resource(params: Dict) -> Iterator[Dict]:
    """
    DLT resource that pages through JobTech API results.
    params includes:
      - q: query string (optional)
      - limit: page size (default 100)
      - occupation_fields: list of occupation field IDs
    """
    url = "https://jobsearch.api.jobtechdev.se/search"
    limit = int(params.get("limit", 100))
    q = params.get("q", "")
    occupation_fields: List[str] = params.get("occupation_fields", OCCUPATION_FIELDS)

    for occ_field in occupation_fields:
        print(f"Fetching data for occupation field: {occ_field}")
        offset = 0

        while True:
            page_params = {"q": q, "limit": limit, "offset": offset, "occupation-field": occ_field}
            data = _get_ads(url, page_params)
            hits = data.get("hits", [])

            if not hits:
                break

            for ad in hits:
                # Normalize ID
                ad["id"] = ad.get("ID", ad.get("id"))
                ad["occupation_field"] = occ_field  # Tag for reference
                yield ad

            if len(hits) < limit or offset > 1900:
                break

            offset += limit


@dlt.source
def jobads_source(q: Optional[str] = "", limit: int = 100):
    """
    DLT source that runs the resource for all occupation fields defined above.
    """
    params = {"q": q, "limit": limit, "occupation_fields": OCCUPATION_FIELDS}
    return jobsearch_resource(params)

# ---- Run the pipeline ----
if __name__ == "__main__":
    print(" Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        destination=dlt.destinations.duckdb("../data_warehouse/job_ads.duckdb"),
        dataset_name="staging.job_ads"
    )
    load_info = pipeline.run(jobads_source())
    print(" Pipeline finished!")
    print(load_info)
