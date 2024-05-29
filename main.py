import json
import time
from pprint import pprint

import requests
import yaml

# os.environ['REQUESTS_CA_BUNDLE'] = "./ca_cert.pem"

# Suppress only the single warning from urllib3 needed.
requests.urllib3.disable_warnings()

# override the methods to set verify=False
requests.get = lambda url, **kwargs: requests.request(method="GET", url=url, verify=False, **kwargs)
requests.post = lambda url, **kwargs: requests.request(method="POST", url=url, verify=False, **kwargs)
requests.put = lambda url, **kwargs: requests.request(method="PUT", url=url, verify=False, **kwargs)
requests.delete = lambda url, **kwargs: requests.request(method="DELETE", url=url, verify=False, **kwargs)

with open('auth.yaml', 'r') as file:
    auth_data = yaml.safe_load(file)

YBA_URL = auth_data['YBA_URL']
API_HEADERS = {
    'X-AUTH-YW-API-TOKEN': f"{auth_data['API_KEY']}"
}


# Helper function to print xcluster info
def print_xcluster_info(xcluster_info: json) -> None:
    print('------------------------------')
    print(f"Name: {xcluster_info['name']}")
    print(f"Status: {xcluster_info['status']}")
    print(f"Paused? {xcluster_info['paused']}")
    print(f"Source UUID: {xcluster_info['sourceUniverseUUID']}")
    print(f"Source State: {xcluster_info['sourceUniverseState']}")
    print(f"Target UUID: {xcluster_info['targetUniverseUUID']}")
    print(f"Target State: {xcluster_info['targetUniverseState']}")
    print('')


#
# Helper function that gets the task data of a given task uuid
#
def get_task_data(customer_uuid, task_uuid) -> json:
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/tasks/{task_uuid}",
                        headers=API_HEADERS).json()


#
# Helper function that waits for a given task to complete and updates the console every sleep interval.
#
# Input(s):
# customer_uuid: uuid - the customer UUID
# task_response: json - the task response body (json) from the action
# friendly_name: str - a friendly task name to display in output (optional)
# sleep_interval: int - an interval to sleep while task is running (optional)
#
def wait_for_task(customer_uuid, task_response, friendly_name="UNKNOWN", sleep_interval=15) -> str:
    if 'taskUUID' not in task_response:
        raise RuntimeError(f"ERROR: failed to process '{friendly_name}' no taskUUID? {task_response}")

    task_uuid = task_response['taskUUID']
    resource_uuid = task_response['resourceUUID']

    while True:
        task_status = get_task_data(customer_uuid, task_uuid)
        match task_status['status']:
            case 'Success':
                print(f"Task '{friendly_name}': {task_uuid} finished successfully!")
                return resource_uuid
            case 'Failure':
                failure_message = f"Task '{friendly_name}': {task_uuid} failed, but could not get the failure messages"
                action_failed_response = requests.get(
                    url=f"{YBA_URL}/api/customers/{customer_uuid}/tasks/{task_uuid}/failed", headers=API_HEADERS).json()
                if 'failedSubTasks' in action_failed_response:
                    errors = [subtask['errorString'] for subtask in action_failed_response['failedSubTasks']]
                    failure_message = (f"Task '{friendly_name}': {task_uuid} failed with the following errors: " +
                                       "\n".join(errors))

                raise RuntimeError(failure_message)
            case _:
                print(f"Waiting for '{friendly_name}' (task='{task_uuid}'): {task_status['percent']:.0f}% complete...")
                time.sleep(sleep_interval)


#
# https://api-docs.yugabyte.com/docs/yugabyte-platform/3b0b8530951e6-get-current-user-customer-uuid-auth-api-token
#
def _get_session_info() -> json:
    return requests.get(url=f"{YBA_URL}/api/v1/session_info", headers=API_HEADERS).json()


#
# https://api-docs.yugabyte.com/docs/yugabyte-platform/66e50c174046d-list-universes
#
def _get_universe_by_name(customer_uuid, universe_name: str) -> json:
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes?name={universe_name}",
                        headers=API_HEADERS).json()


#
# https://api-docs.yugabyte.com/docs/yugabyte-platform/3ff7ead3de133-get-xcluster-config
#
def _get_xcluster_configs(customer_uuid, xcluster_uuid) -> json:
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_uuid}",
                        headers=API_HEADERS).json()


#
# https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/2963c1edbb9e9-get-disaster-recovery-config
#
def _get_dr_configs(customer_uuid, dr_uuid) -> json:
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_uuid}",
                        headers=API_HEADERS).json()


#
#
#
def get_configs_by_type(customer_uuid, config_type) -> json:
    response = requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/configs", headers=API_HEADERS).json()
    return list(filter(lambda config: config["type"] == config_type, response))


#
# Helper method to return the universeUUID of the universe from a given name.
#
def get_universe_uuid_by_name(customer_uuid, universe_name: str) -> str:
    universe = next(iter(_get_universe_by_name(customer_uuid, universe_name)), None)
    if universe is None:
        raise RuntimeError(f"ERROR: failed to find a universe '{universe_name}' by name")
    else:
        return universe['universeUUID']


#
#
#
def get_database_namespaces(customer_uuid, universe_uuid, table_type='PGSQL_TABLE_TYPE') -> list:
    response = requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes/{universe_uuid}/namespaces",
                            headers=API_HEADERS).json()
    # pprint(response)
    return list(filter(lambda db: db['tableType'] == table_type, response))


#
# List YSQL Tables
# See: https://api-docs.yugabyte.com/docs/yugabyte-platform/d00ca6d91e3aa-list-all-tables
#
# Input(s)
# - customer_uuid: uuid - the customer uuid
# - universe_uuid: uuid - the universe uuid
# - include_parent_table_info: bool - ?
# - only_supported_for_xcluster: bool - restricts results to only tables supported by xcluster
# - dbs_list: list<str> - list of database names to include (filter any not matching); default None
#
def _get_all_ysql_tables_list(customer_uuid, universe_uuid, include_parent_table_info=False,
                              only_supported_for_xcluster=True, dbs_list=None) -> json:
    response = requests.get(url=(f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes/{universe_uuid}/tables"
                                 f"?includeParentTableInfo={str(include_parent_table_info).lower()}"
                                 f"&onlySupportedForXCluster={str(only_supported_for_xcluster).lower()}"),
                            headers=API_HEADERS).json()
    # pprint(response)
    if dbs_list is None:
        return list(filter(lambda t: t['tableType'] == 'PGSQL_TABLE_TYPE', response))
    else:
        return list(filter(lambda t: t['tableType'] == 'PGSQL_TABLE_TYPE' and t['keySpace'] in dbs_list, response))


#
# Create a xCluster DR config.
# See: https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/d8cf017de217e-create-disaster-recovery-config
#
# Input:
# - customer_uuid: string<uuid> - a customer uuid
# - _storage_config_uuid: string<uuid> - a storage config for backup/restore of data from source to target
# - source_universe_uuid: string<uuid> - the source universe uuid
# - target_universe_uuid: string<uuid> - the target universe uuid
# - dbs_include_list: list<string<uuid>> - list of database uuids to include in DR replication (defaults to None)
# - parallelism: int - number of parallel threads used for backup/restore (optional, defaults to 8)
# - dry_run: bool - flag to enable "dry run" mode (optional, defaults to False)
#
# Output:
# - resourceUUID: string<uuid> - UUID of the resource being modified by the task
# - taskUUID: string<uuid> - Task UUID
#
def _create_dr_config(customer_uuid, _storage_config_uuid, source_universe_uuid, target_universe_uuid,
                      dbs_include_list=None, parallelism=8, dry_run=False) -> json:
    disaster_recovery_create_form_data = {
        "bootstrapParams": {
            "backupRequestParams": {
                "parallelism": parallelism,
                "storageConfigUUID": _storage_config_uuid
            }
        },
        "dbs": dbs_include_list,
        "dryRun": dry_run,
        "name": f"DR-config-{source_universe_uuid}-to-{target_universe_uuid}",
        "sourceUniverseUUID": source_universe_uuid,
        "targetUniverseUUID": target_universe_uuid
    }
    # pprint(disaster_recovery_create_form_data)

    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs",
                         json=disaster_recovery_create_form_data, headers=API_HEADERS).json()


#
# Delete a xCluster DR config.
# See: https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/defcf45434fc0-delete-xcluster-config
#
# Input
# - a Customer UUID
# - a DR Config UUID
# - an optional flag to force the delete (default False)
#
# Output
# - an HTTP response code
# - a resourceUUID
# - a taskUUID
#
def _delete_dr_config(customer_uuid, _dr_config_uuid, is_force_delete=False) -> json:
    return requests.delete(
        url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{_dr_config_uuid}?isForceDelete={json.dumps(is_force_delete)}",
        headers=API_HEADERS).json()


#
# https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/570cb66189f0d-set-tables-in-disaster-recovery-config
#
def _set_tables_in_dr_config(customer_uuid, _dr_config_uuid, storage_config_uuid, tables_include_list=None,
                             auto_include_indexes=True, parallelism=8) -> json:
    disaster_recovery_set_tables_form_data = {
        "autoIncludeIndexTables": auto_include_indexes,
        "bootstrapParams": {
            "backupRequestParams": {
                "parallelism": parallelism,
                "storageConfigUUID": storage_config_uuid
            }
        },
        "tables": tables_include_list
    }
    # pprint(disaster_recovery_set_tables_form_data)

    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{_dr_config_uuid}/set_tables",
                         json=disaster_recovery_set_tables_form_data, headers=API_HEADERS).json()


#
#
#
def pause_xcluster_config(customer_uuid, xcluster_config_uuid) -> json:
    xcluster_replication_edit_form_data = {
        "status": "Paused"
    }
    return requests.put(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_config_uuid}",
                        json=xcluster_replication_edit_form_data, headers=API_HEADERS).json()


#
#
#
def resume_xcluster_config(customer_uuid, xcluster_config_uuid) -> json:
    xcluster_replication_edit_form_data = {
        "status": "Running"
    }
    return requests.put(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_config_uuid}",
                        json=xcluster_replication_edit_form_data, headers=API_HEADERS).json()


#
# For a given source universe returns the xcluster dr configuration associated with it.
#
def get_source_xcluster_dr_config(customer_uuid, source_universe_name) -> json:
    get_source_universe_response = _get_universe_by_name(customer_uuid, source_universe_name)
    source_universe_details = next(iter(get_source_universe_response), None)
    if source_universe_details is None:
        raise RuntimeError(f"ERROR: the universe '{source_universe_name}' was not found.")
    else:
        dr_config_source_uuid = next(iter(source_universe_details['drConfigUuidsAsSource']), None)
        if dr_config_source_uuid is None:
            raise RuntimeError(f"ERROR: the universe '{source_universe_name}' does not have a DR config.")
        else:
            return _get_dr_configs(customer_uuid, dr_config_source_uuid)


#
#
#
def create_xcluster_dr(customer_uuid, source_universe_name, target_universe_name, db_names=None, dry_run=False) -> json:
    storage_configs = get_configs_by_type(customer_uuid, 'STORAGE')
    # pprint(storage_configs)
    if len(storage_configs) < 1:
        raise RuntimeError('WARN: no storage configs found, at least one is required for xCluster DR setup!')

    storage_config_uuid = storage_configs[0]['configUUID']  # todo how do we select this at scale?
    # pprint(storage_config_uuid)

    get_source_universe_response = _get_universe_by_name(customer_uuid, source_universe_name)
    source_universe_details = next(iter(get_source_universe_response), None)
    # pprint(universe_details)
    if source_universe_details is None:
        raise RuntimeError(f"ERROR: the universe '{source_universe_name}' was not found")

    source_universe_uuid = source_universe_details['universeUUID']
    # pprint(source_universe_uuid)

    dr_config_source_uuid = next(iter(source_universe_details['drConfigUuidsAsSource']), None)
    if dr_config_source_uuid is not None:
        # dr_config = get_dr_configs(session_uuid, dr_config_source_uuid)
        # pprint(dr_config)
        raise RuntimeError(f"WARN: the source universe '{source_universe_name}' already has a disaster-recovery config:"
                           f" {dr_config_source_uuid},")

    target_universe_response = _get_universe_by_name(customer_uuid, target_universe_name)
    target_universe_details = next(iter(target_universe_response), None)
    # pprint(target_universe_details)
    if target_universe_details is None:
        raise RuntimeError(f"ERROR: the target universe '{target_universe_name}' was not found")

    target_universe_uuid = target_universe_details['universeUUID']
    # pprint(target_universe_uuid)

    dbs_list = get_database_namespaces(customer_uuid, source_universe_uuid)
    dbs_list_uuids = [d['namespaceUUID'] for d in dbs_list if d['name'] in db_names]
    # pprint(dbs_list_uuids)

    create_dr_response = _create_dr_config(customer_uuid, storage_config_uuid,
                                           source_universe_uuid, target_universe_uuid,
                                           dbs_list_uuids, dry_run=dry_run)
    wait_for_task(customer_uuid, create_dr_response, 'Create xCluster DR')

    dr_config_uuid = create_dr_response['resourceUUID']
    print(f"SUCCESS: created disaster-recovery config {dr_config_uuid}")
    return dr_config_uuid


#
# Delete an xCluster DR configuration.
# This method can be externalized as it simplifies use of the underlying YBA API.
#
# Input(s)
# - customer_uuid: str - the customer uuid
# - source_universe_name: str - the name of the source universe
#
def delete_xcluster_dr(customer_uuid, source_universe_name) -> str:
    get_universe_response = _get_universe_by_name(customer_uuid, source_universe_name)
    universe_details = next(iter(get_universe_response), None)
    if universe_details is None:
        raise RuntimeError(f"ERROR: source universe '{source_universe_name}' was not found!")

    dr_config_source_uuid = next(iter(universe_details["drConfigUuidsAsSource"]), None)
    if dr_config_source_uuid is None:
        raise RuntimeError(
            f"ERROR: source universe '{source_universe_name}' does not have a disaster-recovery config!")

    response = _delete_dr_config(customer_uuid, dr_config_source_uuid)
    dr_config_uuid = wait_for_task(customer_uuid, response, "Delete xCluster DR")
    print(f"SUCCESS: deleted disaster-recovery config '{dr_config_uuid}'.")
    return dr_config_uuid


#
# For a given universe name, returns a list of database tables not already included in the
# current xcluster dr config.  These are the tables that can be added to the configuration.
# Ideally, these should have sizeBytes = 0 or including it will trigger a full backup/restore
# of the existing database (this will slow the process down).
#
# TODO this list should be compared to the target cluster to make sure the exact same table(s)
# already exist there too.
#
def get_xcluster_dr_available_tables(customer_uuid, universe_name) -> list:
    universe_uuid = get_universe_uuid_by_name(customer_uuid, universe_name)
    all_tables_list = _get_all_ysql_tables_list(customer_uuid, universe_uuid)
    # pprint(all_tables_list)

    # get the current xcluster dr list of included table ids (this is a child entry of the dr config)
    _xcluster_dr_existing_tables_id = get_source_xcluster_dr_config(customer_uuid, universe_name)['tables']
    # pprint(_xcluster_dr_existing_tables_id)

    # filter out any tables whose ids are not already in the current xcluster dr config
    available_tables_list = [t for t in all_tables_list if t['tableID'] not in _xcluster_dr_existing_tables_id]
    # pprint(filter_list)

    return available_tables_list


#
# Adds a set of tables to replication in an existing xCluster DR config.
#
# See also: https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/570cb66189f0d-set-tables-in-disaster-recovery-config
#
# Input(s)
# - customer_uuid: str - the customer uuid
# - source_universe_name: str - the name of the source universe
# - add_tables_ids: set<str> - a set of table ids to add to replication
#
def add_tables_to_xcluster_dr(customer_uuid: str, source_universe_name: str, add_tables_ids: set):
    xcluster_dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(xcluster_dr_config)
    xcluster_dr_uuid = xcluster_dr_config['uuid']
    storage_config_uuid = xcluster_dr_config['bootstrapParams']['backupRequestParams']['storageConfigUUID']

    # get the list of table(s) that can be added to the xCluster DR
    available_dr_tables = get_xcluster_dr_available_tables(customer_uuid, source_universe_name)
    # pprint(available_dr_tables)

    # only include tables that match the provided add_tables_ids
    filtered_dr_tables_list = list(filter(lambda t: t['tableID'] in add_tables_ids, available_dr_tables))
    # pprint(filtered_dr_tables_list)
    if len(filtered_dr_tables_list) == 0:
        raise RuntimeError(f"🤯: no table(s) can be found to add to the xCluster DR config!")

    # TODO should WARN (or ERROR) if source table has a size > 0
    _validate_dr_replica_tables(customer_uuid, xcluster_dr_config['drReplicaUniverseUuid'], filtered_dr_tables_list)

    merged_dr_tables_list = xcluster_dr_config['tables'] + [t['tableID'] for t in available_dr_tables]
    # pprint(merged_dr_tables_list)

    resp = _set_tables_in_dr_config(customer_uuid, xcluster_dr_uuid, storage_config_uuid, merged_dr_tables_list)
    wait_for_task(customer_uuid, resp, "Add YSQL Tables")


def _validate_dr_replica_tables(customer_uuid, dr_replica_universe_uuid, filtered_dr_tables_list):
    replica_tables_list = _get_all_ysql_tables_list(customer_uuid, dr_replica_universe_uuid)
    # pprint(replica_tables_list)
    # error if the replica cluster does not already have the same table(s)
    for table in filtered_dr_tables_list:
        found = False
        for replica_table in replica_tables_list:
            if (table['keySpace'] == replica_table['keySpace'] and
                    table['pgSchemaName'] == replica_table['pgSchemaName'] and
                    table['tableName'] == replica_table['tableName']):
                found = True
        if not found:
            raise RuntimeError(
                f"ERROR: No matching table: '{table['keySpace']}'.'{table['pgSchemaName']}'.'{table['tableName']}'"
                f" found in the target xCluster DR replica!")


#
# Removes a set of tables from replication from an existing xCluster DR config.
#
# See also: https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/570cb66189f0d-set-tables-in-disaster-recovery-config
#
# Input(s)
# - customer_uuid: str - the customer uuid
# - source_universe_name: str - the name of the source universe
# - remove_tables_ids: set<str> - a set of table ids to remove
#
# If no table ids can be removed from the existing config an exception will be raised.
#
def remove_tables_from_xcluster_dr(customer_uuid: str, source_universe_name: str, remove_tables_ids: set):
    xcluster_dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(xcluster_dr_config)
    xcluster_dr_uuid = xcluster_dr_config['uuid']
    storage_config_uuid = xcluster_dr_config['bootstrapParams']['backupRequestParams']['storageConfigUUID']

    filtered_dr_tables_list = list(filter(lambda t: t not in remove_tables_ids, xcluster_dr_config['tables']))
    # pprint(filtered_dr_tables_list)

    if len(filtered_dr_tables_list) == len(xcluster_dr_config['tables']):
        raise RuntimeError(f"ERROR: no table(s) can be removed from the xCluster DR config!")

    resp = _set_tables_in_dr_config(customer_uuid, xcluster_dr_uuid, storage_config_uuid, filtered_dr_tables_list)
    wait_for_task(customer_uuid, resp, "Removing YSQL Tables from xCluster DR")


def testing():
    # Testing Section
    # ---------------
    user_session = _get_session_info()
    customer_uuid = user_session['customerUUID']
    source_universe_name = 'ssherwood-xcluster-east'
    target_universe_name = 'ssherwood-xcluster-central'
    include_database_names = {'yugabyte', 'yugabyte2'}
    test_task_uuid = 'aac2ded4-0b54-4818-affd-fc8ca40c69b1'
    add_list = {'00004000000030008000000000004002'}
    remove_list = {'00004000000030008000000000004002'}

    # todo, show how to get the xcluster uuid from the dr config
    execute_option = 'add_tables_to_xcluster_dr'

    try:
        universe_uuid = get_universe_uuid_by_name(customer_uuid, source_universe_name)

        match execute_option:
            case 'test':
                dbs_list = get_database_namespaces(customer_uuid, universe_uuid)
                pprint(dbs_list)
            case '_get_all_ysql_tables_list':
                tables_list = _get_all_ysql_tables_list(customer_uuid, universe_uuid)
                pprint(tables_list)
            case 'get-available-xcluster-dr-tables-list':
                available_xcluster_dr_tables = get_xcluster_dr_available_tables(customer_uuid, source_universe_name)
                pprint(available_xcluster_dr_tables)
            case 'add_tables_to_xcluster_dr':
                add_tables_to_xcluster_dr(customer_uuid, source_universe_name, add_list)
            case 'remove_tables_from_xcluster_dr':
                remove_tables_from_xcluster_dr(customer_uuid, source_universe_name, remove_list)
            case 'get-xcluster-dr':
                pprint(get_source_xcluster_dr_config(customer_uuid, source_universe_name))
            case 'create-xcluster-dr':
                create_xcluster_dr(customer_uuid, source_universe_name, target_universe_name, include_database_names)
            case 'delete-xcluster-dr':
                delete_xcluster_dr(customer_uuid, source_universe_name)
            case 'get-task-data':
                pprint(get_task_data(customer_uuid, test_task_uuid))
            case 'pause-dr-xcluster':
                pause_xcluster_config(customer_uuid, '')
            case 'resume-dr-xcluster':
                resume_xcluster_config(customer_uuid, '')
    except Exception as ex:
        print(ex)


testing()

# DDL handling: https://docs.yugabyte.com/preview/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/

# DDL flow

# Create a Table
# https://docs.yugabyte.com/preview/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/
#
# 1. Create Table(s) + Index(es) on Primary - NO DATA
# 2. Create Table(s) + Index(es) on Secondary - NO DATA
# 3. Add table(s) UUIDs to the managed API?
# 4. Once replication is confirmed, load data if needed
#
# NOTE: if any data is in the source/target this will trigger a full copy!
#


# SPECIAL RULES FOR COLOCATION!
