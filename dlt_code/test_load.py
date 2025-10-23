import dlt
import requests
import json

dlt.config["load.truncate_staging_dataset"] = True

table_name = "job_ads"

# Example occupation fields
occupation_fields = ("yhCP_AqT_tns", "Uuf1_GMh_Uvw", "9puE_nYg_crq")


def _get_ads(url_for_search, params):
    headers = {"accept": "application/json"}
    response = requests.get(url_for_search, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


@dlt.resource(table_name=table_name, write_disposition="merge", primary_key="ID")
def jobsearch_resource(query: str, occupation_field: str, limit: int = 100):
    """
    params should include at least:
      - "q": your query
      - "limit": page size (e.g. 100)
      - "occupation-field": occupation field code
    """
    url = "https://jobsearch.api.jobtechdev.se/search"
    offset = 0

    while True:
        params = {"q": query, "limit": limit, "offset": offset, "occupation-field": occupation_field}
        data = _get_ads(url, params)

        hits = data.get("hits", [])
        if not hits:
            break

        for ad in hits:
            yield ad

        if len(hits) < limit or offset > 1900:
            break

        offset += limit


@dlt.source
def jobads_source(query: str, occupation_field: str):
    # Call the resource with dynamic parameters
    return jobsearch_resource(query=query, occupation_field=occupation_field)
