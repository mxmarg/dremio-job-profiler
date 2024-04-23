import base64
from io import BytesIO
import json
import logging
import os
import re
import traceback
import zipfile
from uuid import UUID
import csv
import os

import pandas as pd
import numpy as np


filepath_dir = os.path.dirname(os.path.abspath(__file__))


def get_operator_type_mapping(path_to_mapping_file='operator_type_mapping.csv') -> dict:
    """
    Returns a dictionary for mapping the ID operatorType to the operator name displayed in the profile UI.
    E.g. operatorType 10 maps to 'PROJECT'
    """

    path = os.path.join(filepath_dir, path_to_mapping_file)
    with open(path, mode='r') as input_file:
        reader = csv.reader(input_file)
        next(reader)    # Skip header
        operator_mapping = {
            rows[0]: rows[2] for rows in reader
            }

    return operator_mapping



logger = logging.getLogger(__name__)

operator_map = get_operator_type_mapping()

def generate_job_profile_tables(job_id, datazipfile):
    """
    Iterates through a storage container of job profiles and writes parquet files for the three granularity levels of job profiles:
        - jobs_index:       1 entry per job attempt (multiple job attempts per job profile possible)
        - jobs_metrics:     1 entry per job attempt x phase x thread
        - jobs_operators:   1 entry per job attempt x phase x thread x operator
    """

    job_index_rows: list[dict] = []
    job_metrics_rows: list[dict] = []
    job_operators_rows: list[dict] = []
    try:
        try:
            job_index, job_metric, job_operator = analyze_job_profile(datazipfile, job_id)
            job_index_rows.extend(job_index)
            job_metrics_rows.extend(job_metric)
            job_operators_rows.extend(job_operator)
        except Exception as e:
            try:
                with zipfile.ZipFile(datazipfile) as zip_file:
                    with zip_file.open("header.json") as header_file:
                        data = header_file.read()
                        header: dict = json.loads(data.decode("utf-8"))
                        dremio_version = header.get('dremioVersion', "<NOT_FOUND>")
            except:
                dremio_version = "<NOT_FOUND>"
            logger.error(f"[{job_id}] {e} - Profile Dremio version {dremio_version} may not be supported")

    except Exception as e:
        logger.error(f"Affected profile for repro: {job_id}")
        traceback.print_exc()
    
    return job_index_rows, job_metrics_rows, job_operators_rows


def analyze_job_profile(datazipfile: bytes, blob_name: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns information from the input job profile on three granularity levels:
        - jobs_index:       1 entry per job attempt (multiple job attempts per job profile possible)
        - jobs_metrics:     1 entry per job attempt x phase x thread
        - jobs_operators:   1 entry per job attempt x phase x thread x operator 
    """

    job_index_rows: list[dict] = []
    job_metrics_rows: list[dict] = []
    job_operators_rows: list[dict] = []

    try:
        with zipfile.ZipFile(BytesIO(datazipfile)) as zip_file:
            try:
                header_row = extract_header_row(zip_file)
            except KeyError as e:
                logger.error(f"Bad header file: {e}")
                return job_index_rows, job_metrics_rows, job_operators_rows
            header_row['zip_file'] = blob_name

            profile_found = False
            for filename in zip_file.namelist():
                if filename == 'header.json':
                    continue
                elif filename.startswith('profile_') or filename.startswith('prepare_profile_attempt_'):
                    profile_found = True
                    with zip_file.open(filename) as f:
                        data = f.read()
                        profile_data: dict = json.loads(data.decode("utf-8"))
                        # Build job index
                        if not header_row.get('job_id'):
                            try:
                                header_row['job_id'] = extract_job_id_from_filename(blob_name)
                            except ValueError:
                                logger.warning(f'Invalid profile ID {blob_name} - Job ID not found')

                        header_row['dremio_version'] = profile_data['foreman'].get('dremioVersion', '')
                        header_row['sql'] = trim_string_if_too_long(profile_data['query'], max_len=30000)
                        header_row['user'] = profile_data['user']
                        header_row['job_start_time'] = np.datetime64(profile_data['start'], 'ms')
                        if profile_data.get('end'):
                            header_row['job_end_time'] = np.datetime64(profile_data['end'], 'ms')
                            header_row['job_duration'] = profile_data['end'] - profile_data['start']
                        else:
                            header_row['job_end_time'] = np.datetime64("NaT")
                            header_row['job_duration'] = -1
                        header_row['error'] = trim_string_if_too_long(profile_data.get('error', ""), max_len=30000)
                        header_row['failure_info'] = trim_string_if_too_long(profile_data.get('verboseError', ""), max_len=30000)

                        header_row['profile_attempt_file'] = filename
                        attempt_match = re.search(r'\d+', filename)
                        header_row['attempt'] = int(attempt_match.group()) if attempt_match else None
                        header_row['is_prepare'] = filename.startswith('prepare_profile_attempt_')
                        header_row['has_error'] = 'log_attempt_' + str(header_row['attempt']) + '.json' in zip_file.namelist()

                        job_index_row = header_row.copy()
                        job_index_row = extract_job_index_row(profile_data, job_index_row)
                        job_index_row = insert_convert_to_rel_summary(profile_data, job_index_row)
                        job_index_rows.append(job_index_row)

                        # Build job_metrics
                        job_metrics_file_row = {
                            'job_id': job_index_row['job_id'],
                            'submission_id': job_index_row['submission_id'],
                            'attempt': job_index_row['attempt'],
                            'zip_file': job_index_row['zip_file'],
                            'job_start_time': job_index_row['job_start_time']
                        }
                        operator_lookup = create_job_specific_operator_lookup(profile_data)


                        for phase in profile_data['fragmentProfile']:
                            phase_id = phase['majorFragmentId']
                            job_metrics_file_row['majorFragmentId'] = phase_id
                            job_metrics_file_row['downstream_phase'] = operator_lookup['downstream_phase_lookup'].get(phase_id, -1)
                            upstream_source_lookup = operator_lookup['upstream_source_lookup'].get(phase_id, {})
                            if upstream_source_lookup.get('has_leaf'):
                                job_metrics_file_row['upstream_source'] = upstream_source_lookup['plan_operator_name']
                            else:
                                job_metrics_file_row['upstream_source'] = ''
                            job_metrics_file_row['nodePhaseProfile'] = phase['nodePhaseProfile']
                            job_metrics, job_operators = extract_thread_job_metrics_from_phase(job_metrics_file_row, phase, operator_lookup)
                            job_metrics_rows.extend(job_metrics)
                            job_operators_rows.extend(job_operators)
                        f.close()
            if not profile_found:
                logger.error(f"No profile_attempt.json file found")
    except zipfile.BadZipFile as e:
        logger.error(f'Bad zip file: {e}')

    return job_index_rows, job_metrics_rows, job_operators_rows


def extract_job_id_from_filename(filename: str):
    """
    Extracts UUID from job profile filename
    Throws ValueError if filename does not contain valid v4 UUID
    """

    filename = os.path.basename(filename)

    job_id_pattern = re.compile(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})')
    match = job_id_pattern.findall(filename)
    if match:
        job_id = match[0]
    else:
        job_id = filename

    # Throws ValueError if filename not valid v4 UUID
    UUID(job_id, version=4)

    return job_id


def extract_job_index_row(profile_data: dict, job_index_row: dict):
    """
    Extracts information from the input job profile on the first (and highest) granularity level:
        - jobs_index:       1 entry per job attempt (multiple job attempts per job profile possible)
    """
    job_index_row['client_name'] = profile_data.get('clientInfo', {}).get('name', "")
    job_index_row['client_version'] = profile_data.get('clientInfo', {}).get('version', "")
    job_index_row['total_fragments'] = profile_data.get('totalFragments', -1)
    job_index_row['finished_fragments'] = profile_data.get('finishedFragments', -1)
    job_index_row['state'] = profile_data['state']
    job_index_row['foreman'] = profile_data['foreman']

    scheduling_info: dict = profile_data.get('resourceSchedulingProfile')
    if scheduling_info:
        job_index_row['query_type'] = scheduling_info['schedulingProperties']['queryType']
        job_index_row['original_cost'] = scheduling_info['schedulingProperties']['queryCost']
        job_index_row['resource_query_cost'] = scheduling_info.get('queryCost', 0)
        job_index_row['queue_name'] = scheduling_info.get('queueName', "")
        job_index_row['resource_scheduling_start'] = np.datetime64(scheduling_info.get('resourceSchedulingStart'), 'ms')
        job_index_row['resource_scheduling_end'] = np.datetime64(scheduling_info.get('resourceSchedulingEnd'), 'ms')
        job_index_row['queue_resource_scheduling'] = scheduling_info.get('resourceSchedulingEnd', 0) - scheduling_info.get('resourceSchedulingStart', 0)
    else:
        job_index_row['query_type'] = ""
        job_index_row['original_cost'] = 0
        job_index_row['resource_query_cost'] = 0
        job_index_row['queue_name'] = ""
        job_index_row['resource_scheduling_start'] = np.datetime64(None)
        job_index_row['resource_scheduling_end'] = np.datetime64(None)
        job_index_row['queue_resource_scheduling'] = 0

    job_index_row['planning_start'] = np.datetime64(profile_data['planningStart'], 'ms')
    job_index_row['planning_end'] = np.datetime64(profile_data.get('planningEnd'), 'ms')

    job_index_row = aggregate_plan_phases(job_index_row, profile_data['planPhases'])
    job_index_row['planning_normalization_duration'] = profile_data['accelerationProfile'].get('millisTakenNormalizing', 0)
    job_index_row['planning_substitution_duration'] = profile_data['accelerationProfile'].get('millisTakenSubstituting', 0)
    job_index_row['planning_findmat_duration'] = profile_data['accelerationProfile'].get('millisTakenGettingMaterializations', 0)

    job_index_row['command_pool_wait_ms'] = profile_data.get('commandPoolWaitMillis', 0)
    job_index_row['num_plan_cache_used'] = profile_data.get('numPlanCacheUsed', 0)
    try:
        job_index_row['state_durations'] = calculate_state_durations(profile_data.get('stateList', []), job_index_row['job_id'])
    # TODO: Proper exception handling after most edge cases have been handled
    except Exception as e:
        logger.error(f"{job_index_row['job_id']} - {e}")
        traceback.print_exc()
        job_index_row['state_durations'] = {"failed": str(profile_data.get('stateList'))}

    job_index_row['cancel_reason'] = profile_data.get('cancelReason', "")
    job_index_row['nodeProfile'] = profile_data.get('nodeProfile', [])
    if len(job_index_row['nodeProfile']) == 0:
        job_index_row['nodeProfile'] = [
        {"endpoint": {"address": "", "fabricPort": 0},
         "maxMemoryUsed": 0, "numberOfCores": 0, "timeEnqueuedBeforeSubmitMs": 0}
    ]
    dataset_profile = profile_data.get('datasetProfile', [])
    if not dataset_profile:
        dataset_profile = [{"datasetPath": "", "type": -1, "sql": ""}]
    for dataset_path in dataset_profile:
        if "batchSchema" in dataset_path:
            del dataset_path['batchSchema']
        if "sql" in dataset_path:
            dataset_path["sql"] = trim_string_if_too_long(dataset_path["sql"], max_len=30000)

    job_index_row['dataset_profile'] = dataset_profile

    job_index_row['invalid_dataset_metadata_count'] = 1 if "error" in profile_data and "INVALID_DATASET_METADATA" in profile_data['error'] else 0

    if 'accelerationDetails' in profile_data['accelerationProfile']:
        job_index_row['reflections_considered_count'] = len(profile_data['accelerationProfile'].get('layoutProfiles', []))
        details_decoded = base64.b64decode(profile_data['accelerationProfile']['accelerationDetails']).decode('utf-8',errors='ignore')
        job_index_row['acceleration_details'] = trim_string_if_too_long(details_decoded, max_len=30000)

        if 'layoutProfiles' in profile_data['accelerationProfile']:
            profiles = profile_data['accelerationProfile']['layoutProfiles']
        else:
            profiles = [{}]
        if not profiles:
            profiles = [{}]

        layout_profiles = []
        for p in profiles:

            substitutions = [{"plan": trim_string_if_too_long(s.get("plan", ""), 3)} for s in p.get("substitutions", [{"plan": ""}])]
            if not substitutions:
                substitutions.append({"plan": ""})
            normalized_plans = [trim_string_if_too_long(n) for n in p.get("normalizedPlans", [""])]
            if not normalized_plans:
                normalized_plans.append("")
            normalized_query_plans = [trim_string_if_too_long(n) for n in p.get("normalizedQueryPlans", [""])]
            if not normalized_query_plans:
                normalized_query_plans.append("")

            measure_cols = p.get("measureColumns", [])
            if not measure_cols:
                measure_cols. append({"measureType": [""], "name": ""})

            layout_profiles.append({
                "layoutId": p.get("layoutId", ""),
                "materializationId": p.get("materializationId", ""),
                "materializationExpirationTimestamp": p.get("materializationExpirationTimestamp", -1),
                "plan": trim_string_if_too_long(p.get("plan", ""), max_len=30000),
                "dimensions": p.get("dimensions", [""]),
                "measures": p.get("measures", [""]),
                "sortedColumns": p.get("sortedColumns", [""]),
                "partitionedColumns": p.get("partitionedColumns", [""]),
                "distributionColumns": p.get("distributionColumns", [""]),
                "displayColumns": p.get("displayColumns", [""]),
                "numUsed": p.get("numUsed", -1),
                "numSubstitutions": p.get("numSubstitutions", -1),
                "millisTakenSubstituting": p.get("millisTakenSubstituting", -1),
                "substitutions": substitutions,
                "normalizedPlans": normalized_plans,
                "normalizedQueryPlans": normalized_query_plans,
                "name": p.get("name", ""),
                "type": p.get("type", -1),
                "snowflake": p.get("snowflake", False),
                "measureColumns": measure_cols,
                "defaultReflection": p.get("defaultReflection", False)
            })

        job_index_row['layout_profiles'] = layout_profiles
        job_index_row['skipped_mat_due_to_large_join_size'] = 1 if 'due to large Join-Union size during substitutions' in details_decoded else 0
    else:
        job_index_row['reflections_considered_count'] = -1
        job_index_row['skipped_mat_due_to_large_join_size'] = -1
    job_index_row['non_default_options'] = get_non_default_options(profile_data.get('nonDefaultOptionsJSON'))

    return job_index_row


def get_non_default_options(non_default_options_json_str: str):
    non_default_options = json.loads(non_default_options_json_str)

    for non_default_option in non_default_options:
        del non_default_option['kind']
        del non_default_option['type']
        if 'string_val' in non_default_option:
            non_default_option['string_val'] = trim_string_if_too_long(non_default_option['string_val'], max_len=30000)

    # Ensure schema, if list is empty
    if not non_default_options:
        non_default_options = [{
            "name": "", "bool_val": False, "string_val": "", "float_val": 0.0, "int_val": 0
        }]

    return non_default_options


def aggregate_plan_phases(job_index_row, plan_phases):
    planning_phases_duration = 0
    job_index_row['planning_other_phases'] = {}
    partialmeta_duration = 0
    partialmeta_count = 0
    partialmeta_text = ""
    catalog_access_total_datasets = 0
    catalog_access_count = 0
    catalog_access_total_duration = 0
    catalog_access_duration = 0
    catalog_access_max = 0
    catalog_access_max_text = ""
    re_pattern = re.compile(r'Catalog Access for (\d+) Total Dataset\(s\): using (\d+) resolved key\(s\)')

    for phase in plan_phases:
        phase_name = phase['phaseName']
        phase_duration = phase.get('durationMillis', 0)
        if "PERMISSION_CACHE_HIT" in phase_name or "PERMISSION_CACHE_MISS" in phase_name:
            pass
        elif "CACHED_METADATA" in phase_name:
            pass
        elif "PARTIAL_METADATA" in phase_name:
            partialmeta_duration += phase_duration
            partialmeta_count += 1
            partialmeta_text += phase_name + "\n"
        elif "Catalog Access" in phase_name:
            re_match = re_pattern.search(phase_name)
            if re_match:
                catalog_access_total_datasets = re_match.group(1)
                catalog_access_total_duration = phase_duration
            else:
                catalog_access_duration += phase_duration
                catalog_access_count += 1
                if phase_duration > catalog_access_max:
                    catalog_access_max = phase_duration
                    catalog_access_max_text = f"The longest {phase_name} took {phase_duration} ms"
        else:
            planning_phases_duration += phase_duration
            if phase_name == "Convert To Rel":
                job_index_row['planning_convert_to_rel'] = phase_duration
            elif phase_name == "Find Materializations":
                pass
            #     job_index_row['planning_findmat_duration'] = phase_duration
            elif phase_name == "Normalization":
                pass
            #     job_index_row['planning_normalization_duration'] = phase_duration
            elif phase_name == "Substitution":
                pass
            #     job_index_row['planning_substitution_duration'] = phase_duration
            elif phase_name == "Logical Planning":
                job_index_row['planning_logical_duration'] = phase_duration
            elif phase_name == "Physical Planning":
                job_index_row['planning_physical'] = phase_duration
            elif phase_name == "Final Physical Transformation":
                job_index_row['planning_physical_transformation'] = phase_duration
            elif phase_name == "Execution Plan: Fragment Assignment":
                job_index_row['planning_execution_fragment_duration'] = phase_duration
            elif phase_name == "Execution Plan: Plan Generation":
                job_index_row['planning_execution_generation_duration'] = phase_duration
            else:
                job_index_row['planning_other_phases'][phase_name] = phase_duration
    if len(job_index_row['planning_other_phases']) == 0:
        job_index_row['planning_other_phases'] = {'None': -1}

    if (catalog_access_duration != catalog_access_total_duration) \
            or (catalog_access_count != int(catalog_access_total_datasets)):
        catalog_access_max_text += f"\nWARNING: Total reported datasets ({catalog_access_count} vs {catalog_access_total_datasets}) " \
                                   f"and/or total reported duration ({catalog_access_duration} vs {catalog_access_total_duration}) do not match"
    job_index_row['planning_catalog_access_duration'] = catalog_access_duration
    job_index_row['planning_catalog_access_max'] = catalog_access_max
    job_index_row['planning_catalog_access_max_text'] = trim_string_if_too_long(catalog_access_max_text, max_len=30000)

    job_index_row['planning_partialmeta_duration'] = partialmeta_duration
    job_index_row['planning_partialmeta_text'] = trim_string_if_too_long(partialmeta_text, max_len=30000)
    job_index_row['planning_phases_duration'] = planning_phases_duration

    return job_index_row


def get_convert_to_rel(profile_data: dict) -> str:
    plan_phases: list = profile_data.get('planPhases', [])
    convert_to_rel = ""
    for plan_phase in plan_phases:
        if plan_phase.get("phaseName") == "Convert To Rel":
            convert_to_rel = plan_phase.get("plan", "")
            break
    return convert_to_rel


def parse_convert_to_rel(convert_to_rel) -> list[dict]:
    convert_to_rel_parsed = []

    pattern = re.compile(r'(\s*)(\w+)\((.*)\)')
    for match in pattern.finditer(convert_to_rel):
        indent = len(match.group(1))
        operator_name = match.group(2)
        operator_argument = match.group(3)
        convert_to_rel_parsed.append(
            {
                "indent": indent,
                "operator_name": operator_name,
                "operator_argument": operator_argument
            }
        )
    return convert_to_rel_parsed


def summarize_convert_to_rel(convert_to_rel_parsed: list[dict]) -> dict:
    expansion_nodes = set()
    default_expansion_nodes = set()
    for c in convert_to_rel_parsed:
        if c['operator_name'] == "ExpansionNode":
            expansion_nodes.add(c["operator_argument"].replace("path=[", "").replace("]", ""))
        elif c['operator_name'] == "DefaultExpansionNode":
            default_expansion_nodes.add(c["operator_argument"].replace("path=[", "").replace("]", ""))

    convert_to_rel_summary = {
        "ctr_expansion_nodes": list(expansion_nodes),
        "ctr_default_expansion_nodes": list(default_expansion_nodes),
    }
    return convert_to_rel_summary


def insert_convert_to_rel_summary(profile_data: dict, job_index_row):
    convert_to_rel = get_convert_to_rel(profile_data)
    convert_to_rel_parsed = parse_convert_to_rel(convert_to_rel)
    convert_to_rel_summary = summarize_convert_to_rel(convert_to_rel_parsed)
    job_index_row.update(convert_to_rel_summary)

    return job_index_row


def create_job_specific_operator_lookup(profile_data: dict) -> dict[dict]:
    """
    Analyzes the query plan and metrics definitions and returns a dictionary that contains the following two lookup dictionaries:
        - plan_phases_lookup (maps phases to their description, including their tree structure)
        - operator_metrics_lookup (maps metric IDs to operator metrics column names)
    """
    
    plan: str = profile_data.get('plan', "")
    operator_lookup = {}
    plan_phases_lookup = interpret_plan_data(plan)
    operator_lookup['plan_phases_lookup'] = plan_phases_lookup
    operator_lookup['downstream_phase_lookup'] = generate_downstream_phase_lookup(plan_phases_lookup)
    operator_lookup['upstream_source_lookup'] = generate_upstream_source_lookup(plan_phases_lookup)

    metrics_definitions: list = profile_data.get('operatorTypeMetricsMap', {}).get('metricsDef', [])
    operator_metrics_lookup = interpret_metrics_definitions(metrics_definitions)
    operator_lookup['operator_metrics_lookup'] = operator_metrics_lookup

    return operator_lookup


def interpret_plan_data(plan: str) -> dict:
    """
    Creates a mapping of phase + operator number to the operator details.
    The lookup key syntax is '{PHASE_INT}-{OPERATOR_INT}', e.g. '11-3'
    """

    plan_phases_lookup = {}

    pattern = re.compile(r'(?s)(\d{2,})-(\d{2,})(\s+)(\w+)(.*?) : rowType = (.+?): rowcount = (.+?), cumulative cost = {(.+?)}, id = ([+-]?\d+)')
    pattern_cumcost = re.compile(r'(.+?) rows, (.+?) cpu, (.+?) io, (.+?) network, (.+?) memory')
    visualized_plan = {}
    prev_indent = 0
    prev_operator = None

    for match in pattern.finditer(plan):

        phase = int(match.group(1))
        operator = int(match.group(2))
        indent = len(match.group(3))
        operator_name = match.group(4)
        table = trim_string_if_too_long(match.group(5), max_len=30000)
        row_type = trim_string_if_too_long(match.group(6), max_len=30000)
        rowcount = match.group(7)
        cumulative_cost = match.group(8)
        plan_stage_id = match.group(9)

        match_cumcost = pattern_cumcost.search(cumulative_cost)
        if match_cumcost:
            cumcost_rows = match_cumcost.group(1)
            cumcost_cpu = match_cumcost.group(2)
            cumcost_io = match_cumcost.group(3)
            cumcost_network = match_cumcost.group(4)
            cumcost_memory = match_cumcost.group(5)
        elif cumulative_cost == 'tiny':
            cumcost_rows = None
            cumcost_cpu = cumcost_io = cumcost_network = cumcost_memory = '0'
        else:
            logger.warning("No cumulative cost information found in plan substring " + cumulative_cost)
            cumcost_rows = cumcost_cpu = cumcost_io = cumcost_network = cumcost_memory = None

        if prev_indent < indent:
            if prev_indent + 2 != indent and not prev_operator is None:
                plan_row = str(phase) + "-" + str(operator) + indent * " " + operator_name
                logger.warning(f"Unexpected indentation for {plan_row}")
            downstream_operator = prev_operator
        else:
            downstream_operator = visualized_plan[indent-2]
        
        phase_dict = {
            "plan_operator_name": operator_name,
            "plan_table": table,
            "plan_row_type": row_type,
            "plan_rowcount": str(rowcount),
            "plan_cumcost_rows": cumcost_rows,
            "plan_cumcost_cpu": cumcost_cpu,
            "plan_cumcost_io": cumcost_io,
            "plan_cumcost_network": cumcost_network,
            "plan_cumcost_memory": cumcost_memory,
            "plan_stage_id": plan_stage_id,
            "downstream_operator": downstream_operator
        }

        lookup_key = str(phase) + "-" + str(operator)
        plan_phases_lookup[lookup_key] = phase_dict
        
        visualized_plan[indent] = lookup_key
        prev_indent = indent
        prev_operator = lookup_key

    return plan_phases_lookup


def generate_downstream_phase_lookup(plan_phases_lookup: dict) -> dict:
    downstream_phase_lookup = {}
    for key in plan_phases_lookup:
        phase, operator = key.split('-')
        ds_operator = plan_phases_lookup[key]['downstream_operator']
        if ds_operator:
            ds_phase, ds_operator = ds_operator.split('-')
            if ds_phase != phase:
                downstream_phase_lookup[int(phase)] = int(ds_phase)
        elif ds_operator is None:
            downstream_phase_lookup[int(phase)] = None

    return downstream_phase_lookup


def generate_upstream_source_lookup(plan_phases_lookup: dict):
    upstream_source_lookup = {}
    downstream_operators = set()
    for key in plan_phases_lookup:
        downstream_operators.add(plan_phases_lookup[key]['downstream_operator'])
    for key in plan_phases_lookup:
        phase, _ = key.split('-')
        phase = int(phase)
        if key not in downstream_operators:
            upstream_source_lookup[int(phase)] = {
                'has_leaf': True,
                'plan_operator_name': plan_phases_lookup[key]['plan_operator_name']
            }
        else:
            if int(phase) not in upstream_source_lookup:
                upstream_source_lookup[int(phase)] = {'has_leaf': False}

    return upstream_source_lookup


def interpret_metrics_definitions(metrics_definitions: list) -> dict:
    """
    Creates a mapping of operatorType ID to the operator metrics column names, based on an ordered list of metrics definitions.
    
    Example:
    If we assume that the second entry in metrics_definitions is as follows
        metrics_definitions[1] == {"metricDef": [{"id": 0, "name": "N_RECEIVERS"},{"id": 1, "name": "BYTES_SENT"}]}
    We would then receive the according lookup entry with column names in order
        operator_metrics_lookup['1'] == ["N_RECEIVERS", "BYTES_SENT"]
    """

    operator_metrics_lookup = {}
    operator_type = 0
    # The metrics_defintions list is expected to be ordered, with the list index being the operatorType ID
    for metric_def in metrics_definitions:
        columns = []
        id = 0
        for col in metric_def.get('metricDef', []):
            # assert id == col['id']
            columns.append(col['name'])
            id += 1

        operator_metrics_lookup[str(operator_type)] = columns
        operator_type += 1
    return operator_metrics_lookup


def extract_header_row(zip_file: zipfile.ZipFile, parquet_list_items_threshold=999):
    """Extracts job information that is only available in the header.json of the job-profile.zip"""

    header_row = {}
    if "header.json" in zip_file.NameToInfo:
        with zip_file.open("header.json") as header_file:
            data = header_file.read()
            header: dict = json.loads(data.decode("utf-8"))
        header_row['job_id'] = header['job']['info']['jobId']['id']
    else:
        logger.warning("No header.json file found in zipped job profile, significant info fields could not be read.")
        header = {}
        try:
            profile_json_filename = zip_file.filelist[0].filename
            header_row['job_id'] = profile_json_filename.lower().replace("profile_", "").replace(".json", "")
        except Exception as e:
            logger.error(f"No filename found: {e}")
            return
    submission: dict = header.get('submission', {})
    header_row['submission_id'] = submission.get('submissionId', "")
    header_row['submission_date'] = np.datetime64(submission.get('date', 0), 'ms')
    header_row['submission_email'] = submission.get('email', "no email")
    cluster_info: dict = header.get('clusterInfo', {})
    header_row['cluster_info_version'] = cluster_info.get('version', {}).get('version', "")
    header_row['cluster_info_identity'] = cluster_info.get('identity', {}).get('identity', "")
    header_row['sources'] = cluster_info.get('source', [])[:parquet_list_items_threshold]
    header_row['java_vm'] = cluster_info.get('javaVmVersion', "")
    header_row['java_re'] = cluster_info.get('jreVersion', "")
    header_row['edition'] = cluster_info.get('edition', "")

    job_info = header.get('job', {}).get('info', {})
    header_row['request_type'] = job_info.get('requestType', -1)
    header_row['query_type_id'] = job_info.get('queryType', -1)
    header_row['dataset_path'] = job_info.get('datasetPath', [""])
    header_row['dataset_version'] = job_info.get('datasetVersion', "")
    header_row['parents'] = job_info.get('parents', [{"datasetPath": [""], "type": -1}])
    header_row['grand_parents'] = job_info.get('grandParents', [{"datasetPath": [""], "level": -1}])
    header_row['field_origins'] = job_info.get('fieldOrigins', [
        {"name": "", "origins": [{"columnName": "", "derived": False, "table": [""]}]}
    ])[:parquet_list_items_threshold]
    header_row['scan_paths'] = job_info.get('scanPaths', [{"path": [""]}])[:parquet_list_items_threshold]
    header_row['context'] = job_info.get('context', [""])
    header_row['setup_time_ns'] = job_info.get('setupTimeNs', 0)
    header_row['execution_cpu_time_ns'] = job_info.get('executionCpuTimeNs', 0)
    header_row['wait_time_ns'] = job_info.get('waitTimeNs', 0)
    header_row['memory_allocated'] = job_info.get('memoryAllocated', 0)

    resource_scheduling_info: dict = job_info.get('resourceSchedulingInfo', {})
    header_row['queue_name'] = resource_scheduling_info.get('queueName', "")
    header_row['queue_id'] = resource_scheduling_info.get('queueId', "")
    header_row['engine_name'] = resource_scheduling_info.get('engineName', "")
    stats: dict = header.get('job', {}).get('stats', {})
    header_row['input_bytes'] = stats.get('inputBytes', 0)
    header_row['output_bytes'] = stats.get('outputBytes', 0)
    header_row['input_records'] = stats.get('inputRecords', 0)
    header_row['output_records'] = stats.get('outputRecords', 0)
    header_row['is_output_limited'] = stats.get('isOutputLimited', False)
    details: dict = header.get('job', {}).get('details', {})
    header_row['time_spent_in_planning'] = details.get('timeSpentInPlanning', 0)
    header_row['wait_in_client'] = details.get('waitInClient', 0)
    header_row['data_volume'] = details.get('dataVolume', 0)
    header_row['peak_memory'] = details.get('peakMemory', 0)
    header_row['total_memory'] = details.get('totalMemory', 0)
    header_row['cpu_used'] = details.get('cpuUsed', 0)
    header_row['table_dataset_profiles'] = details.get('tableDatasetProfiles', [
        {"datasetProfile": {
            "bytesRead": 0,
            "datasetPaths": [{"datasetPath": [""]}],
            "parallelism": 0,
            "recordsRead": 0,
            "waitOnSource": 0
        }}])
    header_row['top_operations'] = details.get('topOperations', [{"type": 0, "timeConsumed": 0.0}])

    return header_row


def extract_thread_job_metrics_from_phase(job_metrics_file_row: dict, phase: dict, operator_lookup: dict):
    """
    Extracts information from the input job profile on the second and third granularity level:
        - jobs_metrics:     1 entry per job attempt x phase x thread
        - jobs_operators:   1 entry per job attempt x phase x thread x operator 
    """

    job_metrics_rows = []
    job_operators_rows: list[dict] = []

    for thread in phase['minorFragmentProfile']:
        job_metrics_row = job_metrics_file_row.copy()
        job_metrics_row['minorFragmentId'] = thread['minorFragmentId']

        job_operators_rows = extract_operator_job_metrics_from_thread(job_operators_rows, thread,
                                                                      job_metrics_row, operator_lookup)

        job_metrics_row['startTime'] = thread['startTime']
        job_metrics_row['endTime'] = thread['endTime']
        job_metrics_row['lastUpdate'] = thread['lastUpdate']
        job_metrics_row['lastProgress'] = thread['lastProgress']
        job_metrics_row['thread_state'] = thread['state']

        job_metrics_row['memoryUsed'] = thread['memoryUsed']
        job_metrics_row['maxMemoryUsed'] = thread['maxMemoryUsed']
        job_metrics_row['maxIncomingMemoryUsed'] = thread.get('maxIncomingMemoryUsed', 0)
        job_metrics_row['endpointAddress'] = thread['endpoint']['address']
        # endpoint.fabricPort
        job_metrics_row['sleepingDuration'] = thread['sleepingDuration']
        job_metrics_row['blockedDuration'] = thread['blockedDuration']
        job_metrics_row['firstRun'] = thread['firstRun']
        job_metrics_row['runDuration'] = thread['runDuration']
        job_metrics_row['numRuns'] = thread['numRuns']
        job_metrics_row['setupDuration'] = thread['setupDuration']
        job_metrics_row['finishDuration'] = thread['finishDuration']
        job_metrics_row['blockedOnUpstreamDuration'] = thread['blockedOnUpstreamDuration']
        job_metrics_row['blockedOnDownstreamDuration'] = thread['blockedOnDownstreamDuration']
        job_metrics_row['blockedOnSharedResourceDuration'] = thread['blockedOnSharedResourceDuration']
        job_metrics_row['blockedOnMemoryDuration'] = thread.get('blockedOnMemoryDuration', 0)
        job_metrics_row['runQLoad'] = thread.get('runQLoad', 0)
        job_metrics_row['numSlices'] = thread.get('numSlices', 0)
        job_metrics_row['numLongSlices'] = thread.get('numLongSlices', 0)
        job_metrics_row['numShortSlices'] = thread.get('numShortSlices', 0)
        per_resource_blocked_duration = thread['perResourceBlockedDuration']
        job_metrics_row['perResourceBlockedDuration_details'] = rebuild_resource_blocked_map(per_resource_blocked_duration)
        job_metrics_row['perResourceBlockedDuration_UPSTREAM'] = sum_up_resource_blocked(per_resource_blocked_duration, category=0)
        job_metrics_row['perResourceBlockedDuration_DOWNSTREAM'] = sum_up_resource_blocked(per_resource_blocked_duration, category=1)
        job_metrics_row['perResourceBlockedDuration_OTHER'] = sum_up_resource_blocked(per_resource_blocked_duration, category=2)
        job_metrics_row['perResourceBlockedDuration_MEMORY'] = sum_up_resource_blocked(per_resource_blocked_duration, category=3)

        job_metrics_row['peakLocalMemoryAllocatedSum'] = 0
        job_metrics_row['peakLocalMemoryAllocatedMax'] = 0
        job_metrics_row['peakLocalMemoryAllocatedCountOpProf'] = 0
        if 'operatorProfile' in thread:
            peak_local_memory_allocated = [operatorProfile['peakLocalMemoryAllocated']
                                           for operatorProfile in thread['operatorProfile']]
            if peak_local_memory_allocated:
                job_metrics_row['peakLocalMemoryAllocatedSum'] = sum(peak_local_memory_allocated)
                job_metrics_row['peakLocalMemoryAllocatedMax'] = max(peak_local_memory_allocated)
                job_metrics_row['peakLocalMemoryAllocatedCountOpProf'] = len(thread['operatorProfile'])
        job_metrics_rows.append(job_metrics_row)

    return job_metrics_rows, job_operators_rows


def extract_operator_job_metrics_from_thread(job_operators_rows: list, thread: dict,
                                             job_metrics_row: dict, operator_lookup: dict):

    """
    Extracts information from the input job profile on the third (and lowest) granularity level:
        - jobs_operators:   1 entry per job attempt x phase x thread x operator 
    """

    job_operators_header = {
            'job_id': job_metrics_row['job_id'],
            'submission_id': job_metrics_row['submission_id'],
            'attempt': job_metrics_row['attempt'],
            'zip_file': job_metrics_row['zip_file'],
            'job_start_time': job_metrics_row['job_start_time'],
            'majorFragmentId': job_metrics_row['majorFragmentId'],
            'minorFragmentId': job_metrics_row['minorFragmentId']
            }
    for operator in thread['operatorProfile']:
        job_operators_row = job_operators_header.copy()
        job_operators_row.update(operator)
        job_operators_row = rebuild_metrics_map(job_operators_row, operator_lookup)
        operator_type_key = str(operator['operatorType'])
        try:
            job_operators_row['operator_name_frontend'] = operator_map[operator_type_key]
        except KeyError:
            logger.warning(f"Unknown operator type {operator_type_key} in job {job_operators_header['job_id']}, operator {job_metrics_row['majorFragmentId']}-XX-{job_operators_row['operatorId']}")
            job_operators_row['operator_name_frontend'] = None
        job_operators_row = aggregate_input_profile(job_operators_row)

        phase_number = job_operators_row["majorFragmentId"]
        operator_number = job_operators_row["operatorId"]
        lookup_key = str(phase_number) + "-" + str(operator_number)
        try:
            phase_plan_info: dict = operator_lookup['plan_phases_lookup'][lookup_key]
            job_operators_row.update(phase_plan_info)
        except KeyError as e:
            # Operators at position 0 (key: XX-0) are often expected to be missing in the planning stages
            if not ('-0' in str(e)):
                logger.warning(f"KeyError {e} - Job ID {job_operators_row['job_id']} - "
                               f"Operator Type {job_operators_row['operatorType']} "
                               f"at position {phase_number}-XX-{operator_number} not found in lookup")
        job_operators_rows.append(job_operators_row)

    return job_operators_rows


def aggregate_input_profile(job_operators_row: dict) -> dict:
    """Returns an 'inputProfile' dictionary that contains aggregated profile information about the oparator"""
    # TODO: Understand meaning of multiple entries for 'inputProfile' on operator level and possible replace max() with sum()

    input_profile: list = job_operators_row['inputProfile']    
    max_batches = max(input_profile, key=lambda item: item['batches'])
    max_records = max(input_profile, key=lambda item: item['records'])
    max_size = max(input_profile, key=lambda item: item['size'])
    job_operators_row['inputProfile'] = {
        'max_batches': max_batches['batches'],
        'max_records': max_records['records'],
        'max_size': max_size['size']
    }

    return job_operators_row


def trim_string_if_too_long(s: str, max_len: int = 3000):
    if s is not None and len(s) > max_len:
        s = s[:max_len] + '..'
    return s


def ts_subtract(end, start):
    if end is None or start is None:
        return None
    else:
        duration = end - start
        if duration < 0:
            logger.warning(f"Negative duration: {duration} ms")
        return end - start


def calculate_phase(state_list, state_list_properties, index):
    if state_list_properties['cancel_state'] == index + 1:
        return ts_subtract(state_list[-1]['startTime'], state_list[index-1]['startTime'])
    else:
        return ts_subtract(state_list[index]['startTime'], state_list[index-1]['startTime'])


def calculate_metadata_retrieval(state_list, state_list_properties):
    if state_list_properties['cancel_state'] == 3:
        return ts_subtract(state_list[-1]['startTime'], state_list[1]['startTime'])
    elif 3 in state_list_properties['missing_states']:
        return ts_subtract(state_list[3]['startTime'], state_list[1]['startTime'])
    else:
        return ts_subtract(state_list[2]['startTime'], state_list[1]['startTime'])


def calculate_planning(state_list, state_list_properties):
    if state_list_properties['cancel_state'] == 4:
        return ts_subtract(state_list[-1]['startTime'], state_list[2]['startTime'])
    elif 3 in state_list_properties['missing_states']:
        return None
    else:
        return ts_subtract(state_list[3]['startTime'], state_list[2]['startTime'])


def calculate_state_durations(state_list: list[dict], job_id: str) -> dict:
    """Creates a dictionary of the eight state durations listed on the top of the query profile viewer UI"""

    state_list_properties = generate_state_list_properties(job_id, state_list)
    for state in sorted(list(state_list_properties['missing_states'])):
        state_list.insert(state-1, {'state': state, 'startTime': None})

    state_durations = {
        '1-Pending': calculate_phase(state_list, state_list_properties, index=1),
        '2-MetadataRetrieval': calculate_metadata_retrieval(state_list, state_list_properties),
        '3-Planning': calculate_planning(state_list, state_list_properties),
        '4-EngineStart': calculate_phase(state_list, state_list_properties, index=4),
        '5-Queued': calculate_phase(state_list, state_list_properties, index=5),
        '6-ExecutionPlanning': calculate_phase(state_list, state_list_properties, index=6),
        '7-Starting': calculate_phase(state_list, state_list_properties, index=7),
        '8-Running': calculate_phase(state_list, state_list_properties, index=8)
    }

    return state_durations


def generate_state_list_properties(job_id, state_list):
    # The default expected profile states are numbered from 1 to 9
    expected_states = set(range(1, 10))
    present_states = set()
    for state in state_list:
        present_states.add(state['state'])
    missing_states = expected_states - present_states
    special_states = present_states - expected_states
    if len(special_states) > 1:
        logger.warning(f"Multiple unexpected states {special_states} in job ID {job_id}")

    # When a query is cancelled/failed, state 9 is missing
    cancel_state = None
    if missing_states and 9 not in present_states:
        # If state 4 (and above) are missing, find the lowest missing state
        if 4 not in present_states:
            cancel_state = min(missing_states)
        # Otherwise, state 3 is oftentimes missing, even if query was not cancelled
        elif missing_states - {3}:
            cancel_state = min(missing_states - {3})

    state_list_properties = {
        "missing_states": missing_states,
        "cancel_state": cancel_state,
        "special_states": special_states
    }
    return state_list_properties


def rebuild_metrics_map(job_operators_row: dict, operator_lookup: dict) -> dict:
    """Returns a 'metrics' dictionary of OPERATOR_METRIC_NAME -> METRIC_VALUE"""

    operator_type = job_operators_row['operatorType']
    columns: list = operator_lookup['operator_metrics_lookup'][str(operator_type)]
    columns_len = len(columns)
    raw_metrics = job_operators_row['metric']
    metrics_map = {}
    unmatched_metrics = []
    for metric in raw_metrics:
        metric_id = metric['metricId']

        if 'longValue' in metric:
            metric_value = metric['longValue']
        elif 'doubleValue' in metric:
            metric_value = metric['doubleValue']
        else:
            logger.warning(f"Unexpected metric schema and data type {metric}. Value for metric could not be retrieved.")
            metric_value = None

        if metric_id < columns_len:
            metric_name = columns[metric_id]
            metrics_map[metric_name] = metric_value
        else:
            if not metric_value is None:
                logger.debug(f"{job_operators_row['job_id']}: Index {metric_id} for operator type {operator_type} not found in columns {columns}. Metric value {metric_value} could not be matched")
            unmatched_metrics.append(metric)

    if unmatched_metrics:
        metrics_map['unmatched_metrics'] = unmatched_metrics
    job_operators_row['metric'] = metrics_map

    return job_operators_row


def rebuild_resource_blocked_map(per_resource_blocked_duration: list) -> dict:
    """Returns a 'resource_blocked_duration' dictionary of RESOURCE_NAME -> METRIC_VALUE"""

    resource_blocked_map = {}
    for resource_blocked in per_resource_blocked_duration:
        key = resource_blocked['resource']
        value = resource_blocked['duration']
        resource_blocked_map[key] = value

    if len(resource_blocked_map.keys()) == 0:
        resource_blocked_map['empty'] = 0

    return resource_blocked_map


def sum_up_resource_blocked(per_resource_blocked_duration: list, category: int) -> dict:

    sum_total = 0
    for resource_blocked in per_resource_blocked_duration:
        if resource_blocked['category'] == category:
            sum_total += resource_blocked['duration']

    return sum_total
