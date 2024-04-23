import sys
from retrieve_job_profiles import log_in, retrieve_job_profiles


if __name__ == '__main__':
    DREMIO_ENDPOINT = sys.argv[1]
    DREMIO_ADMIN_USERNAME = sys.argv[2]
    DREMIO_PAT_TOKEN = sys.argv[3]
    retrieve_job_profiles(DREMIO_ENDPOINT, DREMIO_ADMIN_USERNAME, DREMIO_PAT_TOKEN)