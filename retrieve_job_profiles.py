import requests, os, urllib3, sys
import pandas as pd

from generate_job_profile_tables import generate_job_profile_tables

urllib3.disable_warnings()

def log_in(DREMIO_ENDPOINT: str, DREMIO_ADMIN_USERNAME: str, DREMIO_PAT_TOKEN: str):
    headers = {"Content-Type": "application/json"}
    payload = '{"userName": "' + DREMIO_ADMIN_USERNAME + '","password": "' + DREMIO_PAT_TOKEN + '"}'
    payload = payload.encode(encoding='utf-8')
    response = requests.request(
        "POST",
        DREMIO_ENDPOINT + "/apiv2/login", 
        data=payload,
        headers=headers,
        timeout=600,
        verify=False
    )
    if response.status_code != 200:
        print("Authentication Error " + str(response.status_code))
        raise RuntimeError("Authentication error.")
    else:
        print("Successfully authenticated")

    token = '_dremio' + response.json()['token']
    return token


def download_job_profile(DREMIO_ENDPOINT: str, auth_token: str, job_id: str):
    headers = {"Accept": "application/octet-stream", "Authorization": auth_token}
    response = requests.request(
        "POST", 
        DREMIO_ENDPOINT + "/apiv2/support/" + job_id + "/download", 
        headers=headers, 
        timeout=None, 
        verify=False
    )
    
    if response.status_code == 200:
        file = open("profiles/" + job_id + ".zip" , "wb")
        file.write(response.content)
        file.close()
    else:
        print("Error " + str(response.status_code) + ": " + response.text)
    
    return response.content


def collect_all_jobs(queries_dir) -> pd.DataFrame:
    queries_dir = 'queries'
    all_files = [os.path.join(queries_dir, f) for f in os.listdir(queries_dir) if os.path.isfile(os.path.join(queries_dir, f))]

    all_jobs_dfs = []
    jobs_df = pd.DataFrame()
    for filename in all_files:
        print("... Reading " + filename)
        
        try:
            df = pd.read_json(
                filename,
                lines=True,
                compression='infer'
            )
        except ValueError as e:
            print(f"ERROR: Unable to read {filename}, likely due to incorrect JSON format. {e}")
            continue
        try:
            filtered = df[df['outcome']=="COMPLETED"]
            filtered = filtered[filtered['requestType'].isin(['RUN_SQL', 'EXECUTE_PREPARE', 'CREATE_PREPARE'])]
            col_filtered = filtered[['queryId', 'queryCost', 'executionPlanningTime', 'runningTime', 'start', 'outcome']]
            all_jobs_dfs.append(col_filtered)
        except KeyError as e:
            print(f"ERROR: Unable to read {filename}, likely due to incorrect schema. Available columns: {list(df.columns)} - Missing Key: {e}")
    if all_jobs_dfs:
        jobs_df = pd.concat(all_jobs_dfs, axis=0, ignore_index=True)
    return jobs_df


def apply_alerting_rules(job_index_rows, job_metrics_rows, job_operators_rows):
    
    is_system_user = False
    for i in job_index_rows:
        if i["user"] == "$dremio$":
            is_system_user = True
        if i["total_fragments"] is not None and (i["total_fragments"] != i["finished_fragments"]):
            print(f"WARNING: Only {i["finished_fragments"]} of {i["total_fragments"]} fragments have reported back")
    
    for m in job_metrics_rows:
        if not is_system_user and m["thread_state"] != 1:
            print(f"WARNING: Thread {m['majorFragmentId']}-{m['minorFragmentId']}-XX has not completed. Thread state {m["thread_state"]}")




def retrieve_job_profiles(DREMIO_ENDPOINT: str, DREMIO_ADMIN_USERNAME: str, DREMIO_PAT_TOKEN: str):
    auth_token = log_in(DREMIO_ENDPOINT, DREMIO_ADMIN_USERNAME, DREMIO_PAT_TOKEN)

    jobs = collect_all_jobs(queries_dir='queries')
    
    for _, row in jobs.iterrows():
        job_id = row['queryId']
        print("... downloading job profile: " + job_id)
        sys.stdout.flush()
        job_zip = download_job_profile(DREMIO_ENDPOINT, auth_token, job_id)

        job_index_rows, job_metrics_rows, job_operators_rows = generate_job_profile_tables(job_id, job_zip)

        apply_alerting_rules(job_index_rows, job_metrics_rows, job_operators_rows)