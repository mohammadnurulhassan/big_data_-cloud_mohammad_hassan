import dlt
import requests
import json
import os


dlt.config["load.truncate_staging_dataset"] = True



 params = {"q": query, "limit": 100, "occupation-field": occupation_field}
query = ""
    table_name = "job_ads"


    occupation_fields = ("yhCP_AqT_tns", "Uuf1_GMh_Uvw", "9puE_nYg_crq")




def _get_ads(url_for_search, params):
    headers = {"accept": "application/json"}
    response = requests.get(url_for_search, headers=headers, params=params)
    response.raise_for_status() 
    return json.loads(response.content.decode("utf8"))


@dlt.resource(table_name="job_ads",write_disposition="merge", primary_key="ID")
def jobsearch_resource(params):
    """
    params should include at least:
      - "q": your query
      - "limit": page size (e.g. 100)
    """
    url = "https://jobsearch.api.jobtechdev.se"
    url_for_search = f"{url}/search"
    limit = params.get("limit", 100)
    offset = 0

    while True:
        # build this page’s params
        page_params = dict(params, offset=offset)
        data = _get_ads(url_for_search, page_params)

        hits = data.get("hits", [])
        if not hits:
            # no more results
            break

        # yield each ad on this page
        for ad in hits:
            yield ad

        # if fewer than a full page was returned, we’re done
        if len(hits) < limit or offset > 1900:
            break

        offset += limit



#dagster works with dlt source not dlt resource

@dlt.source
def jobads_source(q):
    return jobads_resource(params={"q": q, "limit": 100})













































#def run_pipeline(query, table_name, occupation_fields):
   # pipeline = dlt.pipeline(
   #     pipeline_name="jobads_demo",
   #     destination="snowflake",
   #     dataset_name="staging",
   # )
   #
   # for occupation_field in occupation_fields:
   #     params = {"q": query, "limit": 100, "occupation-field": occupation_field}
   #     load_info = pipeline.run(
   #         jobsearch_resource(params=params), table_name=table_name
   #     )
   #     print(f"Occupation field: {occupation_field}")
   #     print(load_info)

   # for occupation_field in occupation_fields:
       
       # load_info = pipeline.run(
       #     jobsearch_resource(params=params), table_name=table_name
       # )
       # print(f"Occupation field: {occupation_field}")
       # print(load_info)


#if __name__ == "__main__":
   # working_directory = Path(__file__).parent
   # os.chdir(working_directory)

    
   # run_pipeline(query, table_name, occupation_fields)
