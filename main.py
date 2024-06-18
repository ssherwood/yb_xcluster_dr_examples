"""
This module provides a collection of helpful methods for interacting and automating the YBA xCluster DR APIs.

Prior to using this module, please review the xCluster DR documentation:
 - https://docs.yugabyte.com/preview/yugabyte-platform/back-up-restore-universes/disaster-recovery/

DDL Handling
------------

- https://docs.yugabyte.com/preview/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/


Create a Table
--------------

https://docs.yugabyte.com/v2.20/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/#add-a-table-to-dr

1. Create Table(s) + Index(es) on Primary - NO DATA
2. Create Table(s) + Index(es) on Secondary - NO DATA
3. Add table(s) UUIDs to the managed API?
4. Once replication is confirmed, load data if needed

Drop a Table
------------

- https://docs.yugabyte.com/v2.20/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/#remove-a-table-from-dr


Add an Index
------------

- https://docs.yugabyte.com/v2.20/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/#add-an-index-to-dr


Drop an Index
-------------

- https://docs.yugabyte.com/v2.20/yugabyte-platform/back-up-restore-universes/disaster-recovery/disaster-recovery-tables/#remove-an-index-from-dr

Additional Notes
----------------

 - If ANY data is in the source/target database tables it will trigger a full copy of the whole database
 - If you try to make a DDL change on DR primary and it fails, you must also make the same attempt on DR replica and get
   the same failure.

Rules for Colocation
--------------------

 - If you are using Colocated tables, you CREATE TABLE on DR primary, then CREATE TABLE on DR replica making sure that
   you force the Colocation ID to be identical to that on DR primary.

:author: Shawn Sherwood
:date: June 2024
"""
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


def _get_task_status(customer_uuid: str, task_uuid: str) -> json:
    """
    Basic function that gets a task's status for a given task UUID in YBA.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/ae83717943b4c-get-a-task-s-status
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/08618836e48aa-customer-task-data

    :param customer_uuid: str - the customer UUID
    :param task_uuid: str - the task's UUID
    :return: json<CustomerTaskData>
    """
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/tasks/{task_uuid}",
                        headers=API_HEADERS).json()


def wait_for_task(customer_uuid: str, task_response, friendly_name="UNKNOWN", sleep_interval=15):
    """
    Utility function that waits for a given task to complete and updates the console every sleep interval.
    On success the return will be final task status.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/08618836e48aa-customer-task-data

    :param customer_uuid: str - the customer UUID
    :param task_response: json<ActionResponse> - the task response body (json) from the action
    :param friendly_name: str - a friendly task name to display in output (optional, default is UNKNOWN)
    :param sleep_interval: int - an interval to sleep while task is running (optional, default 15s)
    :return: json of CustomerTaskData (the final task result)
    :raises RuntimeError: if the task fails or cannot be found
    """
    if 'taskUUID' not in task_response:
        raise RuntimeError(f"ERROR: failed to process '{friendly_name}' no taskUUID? {task_response}")

    task_uuid = task_response['taskUUID']

    while True:
        task_status = _get_task_status(customer_uuid, task_uuid)
        match task_status['status']:
            case 'Success':
                print(f"Task '{friendly_name}': {task_uuid} finished successfully!")
                return task_status
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


def _get_session_info():
    """
    Basic function that gets the current user's session info from YBA. Primarily use this as a convenient way to get
    the current user's `customerUUID`.

    See also: https://api-docs.yugabyte.com/docs/yugabyte-platform/3b0b8530951e6-get-current-user-customer-uuid-auth-api-token

    :return: json of SessionInfo
    """
    return requests.get(url=f"{YBA_URL}/api/v1/session_info", headers=API_HEADERS).json()


def _get_universe_by_name(customer_uuid: str, universe_name: str):
    """
    Basic function that returns a universe by its friendly name.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/66e50c174046d-list-universes
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/8682dd63e9165-universe-resp

    :param customer_uuid: str - the customer UUID
    :param universe_name: str - the friendly name of the universe to be returned
    :return: json array of UniverseResp
    """
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes?name={universe_name}",
                        headers=API_HEADERS).json()


def _get_xcluster_configs(customer_uuid, xcluster_config_uuid):
    """
    Basic function that gets the xCluster configration data for a given xCluster config UUID from YBA.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/3ff7ead3de133-get-xcluster-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/1486cbdec4522-x-cluster-config-get-resp

    :param customer_uuid: str - the customer UUID
    :param xcluster_config_uuid: str - the xCluster Config UUID
    :return: json of XClusterConfigGetResp
    """
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_config_uuid}",
                        headers=API_HEADERS).json()


def _get_xcluster_dr_configs(customer_uuid: str, xcluster_dr_uuid: str) -> json:
    """
    Basic function that gets an xCluster DR configuration for a given DR config UUID.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/2963c1edbb9e9-get-disaster-recovery-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/db4d138705705-dr-config

    :param customer_uuid: str - the customer UUID
    :param xcluster_dr_uuid: str - the DR config UUID to return
    :return: json of DrConfig
    """
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{xcluster_dr_uuid}",
                        headers=API_HEADERS).json()


def _get_configs_by_type(customer_uuid: str, config_type: str):
    """
    Return a Customer's configs of a specific config type. This is useful for getting things like the STORAGE configs.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/d09c43e4a8bfd-list-all-customer-configurations
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/0e51caecbdf07-customer-config

    :param customer_uuid: str - the customer UUID
    :param config_type: enum<str> - the config type (of STORAGE, ALERTS, CALLHOME, PASSWORD_POLICY).
    :return: json array of CustomerConfig
    """
    response = requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/configs", headers=API_HEADERS).json()
    return list(filter(lambda config: config["type"] == config_type, response))


def _get_all_ysql_tables_list(customer_uuid: str, universe_uuid: str, table_type='PGSQL_TABLE_TYPE',
                              include_parent_table_info=False, only_supported_for_xcluster=True, dbs_include_list=None):
    """
    Returns a list of YSQL tables for a given Universe possibly filtered by type and if it is supported by xCluster.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/d00ca6d91e3aa-list-all-tables
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/2419074f53925-table-info-resp

    :param customer_uuid: str - the Customer UUID
    :param universe_uuid: str - the Universe UUID
    :param table_type: str - the type of tables to return
    :param include_parent_table_info: bool - whether to include the parent table information
    :param only_supported_for_xcluster: bool - whether to only include XCluster tables
    :param dbs_include_list: list<str> - list of database names to include (filter out any not matching); default None
    :return: json array of TableInfoResp (possibly filtered)
    """
    response = requests.get(url=(f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes/{universe_uuid}/tables"
                                 f"?includeParentTableInfo={str(include_parent_table_info).lower()}"
                                 f"&onlySupportedForXCluster={str(only_supported_for_xcluster).lower()}"),
                            headers=API_HEADERS).json()
    # pprint(response)
    if dbs_include_list is None:
        return list(filter(lambda t: t['tableType'] == table_type, response))
    else:
        return list(filter(lambda t: t['tableType'] == table_type and t['keySpace'] in dbs_include_list, response))


def _create_dr_config(customer_uuid: str, storage_config_uuid: str, source_universe_uuid: str,
                      target_universe_uuid: str, dbs_include_list=None, parallelism=8, dry_run=False):
    """
    Creates a new xCluster DR config for given source and target universe and a required storage config.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/d8cf017de217e-create-disaster-recovery-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param storage_config_uuid: str - a storage config for backup/restore of data from source to target
    :param source_universe_uuid: str - the source Universe UUID
    :param target_universe_uuid: str - the target Universe UUID
    :param dbs_include_list: list<str> - list of database names to include (filter out any not matching); default None
    :param parallelism: int - the number of parallel threads to use during backup/restore bootstrap; default 8
    :param dry_run: bool - whether to perform as a "dry run"; default False
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    disaster_recovery_create_form_data = {
        "bootstrapParams": {
            "backupRequestParams": {
                "parallelism": parallelism,
                "storageConfigUUID": storage_config_uuid
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


def _delete_xcluster_dr_config(customer_uuid: str, dr_config_uuid: str, is_force_delete=False):
    """
    Deletes an existing xCluster DR config.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/defcf45434fc0-delete-xcluster-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid:  str - the DR config UUID to return
    :param is_force_delete: bool - whether to force delete the DR config; default False
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    return requests.delete(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}"
                               f"?isForceDelete={json.dumps(is_force_delete)}", headers=API_HEADERS).json()


def _set_tables_in_dr_config(customer_uuid: str, dr_config_uuid: str, storage_config_uuid: str,
                             tables_include_set=None, auto_include_indexes=True, parallelism=8):
    """
    Updates the set of tables that are included in the xCluster DR. As this is a POST operation, the tables list should
    always contain the full set of table IDs intended to be used in xCluster DR replication. This also means that to
    effectively remove tables, the POST should contain the existing set of tables minus the tables to be removed.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/570cb66189f0d-set-tables-in-disaster-recovery-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :param storage_config_uuid: str - a storage config UUID for backup/restore of data
    :param tables_include_set: set<str> - list of table UUIDs to include in replication
    :param auto_include_indexes: bool - whether to automatically include indexes of the selected tables; default True
    :param parallelism: int - the number of parallel threads to use during backup/restore bootstrap; default 8
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    disaster_recovery_set_tables_form_data = {
        "autoIncludeIndexTables": auto_include_indexes,
        "bootstrapParams": {
            "backupRequestParams": {
                "parallelism": parallelism,
                "storageConfigUUID": storage_config_uuid
            }
        },
        "tables": tables_include_set
    }
    # pprint(disaster_recovery_set_tables_form_data)

    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/set_tables",
                         json=disaster_recovery_set_tables_form_data, headers=API_HEADERS).json()


def _validate_dr_replica_tables(customer_uuid: str, dr_replica_universe_uuid: str, source_dr_tables_add_list: list):
    """
    Validates that the target xCluster DR (replica) actually contains the table(s) that are going to be added to the
    xCluster DR config of the source.  This causes an error as it can't replicate table(s) that don't already exist on
    the target.

    No additional correctness is checked to ensure the replica's table(s) contains the same fields - this is left up to
    the users to ensure the exact same DDL is being issued to both sides of the xCluster config.

    :param customer_uuid: str - the customer uuid
    :param dr_replica_universe_uuid: str - the uuid of the target (replica) universe
    :param source_dr_tables_add_list: list<dict> - a list of tables to be added to the source DR
    :return:
    """
    keys_to_match = ['keySpace', 'pgSchemaName', 'tableName']  # these keys define a unique table
    replica_tables_list = _get_all_ysql_tables_list(customer_uuid, dr_replica_universe_uuid)
    replica_tables_set = {tuple(d[key] for key in keys_to_match) for d in replica_tables_list}
    tables_not_found = [
        t for t in source_dr_tables_add_list if tuple(t[key] for key in keys_to_match) not in replica_tables_set
    ]
    # pprint(tables_not_found)
    if len(tables_not_found) > 0:
        raise RuntimeError(f"ERROR: No matching table(s): {tables_not_found} found in the xCluster DR replica!")


def _switchover_xcluster_dr(customer_uuid: str, dr_config_uuid: str, primary_universe_uuid: str,
                            dr_replica_universe_uuid: str):
    """
    Initiates an xCluster DR "planned" switchover.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/066dda1e654a3-switchover-a-disaster-recovery-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :param primary_universe_uuid: str - the primary Universe UUID
    :param dr_replica_universe_uuid: str - the secondary (replica) Universe UUID
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    disaster_recovery_switchover_form_data = {
        'primaryUniverseUuid': primary_universe_uuid,
        'drReplicaUniverseUuid': dr_replica_universe_uuid,
    }
    # pprint(disaster_recovery_switchover_form_data)
    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/switchover",
                         json=disaster_recovery_switchover_form_data, headers=API_HEADERS).json()


def _get_xcluster_dr_safetime(customer_uuid: str, dr_config_uuid: str):
    """
    Get the xCluster DR config safe times. This information is needed to pass into the _failover_xcluster_dr method.

    See also:
     - <can't find these docs yet>

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :return: json<DrConfigSafeTimeResp>
    """
    return requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/safetime",
                        headers=API_HEADERS).json()


def _failover_xcluster_dr(customer_uuid: str, dr_config_uuid: str, primary_universe_uuid: str,
                          dr_replica_universe_uuid: str, namespace_id_safetime_epoch_us_map: dict):
    """
    Initiates an xCluster DR fail-over after an "unplanned" failure of the Primary cluster/region.

    NOTE: it is anticipated that, in this failure scenario, it may first require an HA switchover of YBA itself before
    being able to run this operation. This may also require changing the underlying YBA_URL used if the HA instance is
    not already behind a load balancer.

    After this operation is successful, the DR Replica becomes the new Primary cluster without an automatic DR
    configuration. Once the Primary has recovered and is accessible again, run the Restart (Repair) xCluster DR
    operations and, once completed, optionally do a DR Switchover to return back to the original Primary DR.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/a3bcb16787481-failover-a-disaster-recovery-config

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :param primary_universe_uuid: str - the primary Universe UUID
    :param dr_replica_universe_uuid: str - the secondary (replica) Universe UUID
    :param namespace_id_safetime_epoch_us_map: dict<str, int> - the current epoch safetimes for the DR config
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    disaster_recovery_failover_form_data = {
        "primaryUniverseUuid": primary_universe_uuid,
        "drReplicaUniverseUuid": dr_replica_universe_uuid,
        "namespaceIdSafetimeEpochUsMap": namespace_id_safetime_epoch_us_map,
    }
    # pprint(disaster_recovery_failover_form_data)
    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/failover",
                         json=disaster_recovery_failover_form_data, headers=API_HEADERS).json()


def _restart_xcluster_dr_config(customer_uuid: str, dr_config_uuid: str, dbs_list=None, is_force_delete=False):
    """
    Restarts a failed xCluster DR after a fail-over event.

    The underlying API allows for the use of a different bootstrap params and dbs list, but both appear to be completely
    optional - they default to the original DR config's settings which seems logical.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/9bc18aeb584ec-restart-disaster-recovery-config

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :param is_force_delete: bool - force delete of what?  Optional, default is False
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    disaster_recovery_restart_form_data = {
        'dbs': dbs_list or []
    }
    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/restart"
                             f"?isForceDelete={json.dumps(is_force_delete)}",
                         json=disaster_recovery_restart_form_data, headers=API_HEADERS).json()


def _sync_xcluster_dr_config(customer_uuid: str, dr_config_uuid: str):
    """
    Call to ensure changes made outside YBA are reflected and resynchronize the YBA UI.  This is typically used when
    adding/removing indexes from DR.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/10ad9b184b8d4-sync-disaster-recovery-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param dr_config_uuid: str - the DR config UUID to use
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    return requests.post(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/dr_configs/{dr_config_uuid}/sync",
                         headers=API_HEADERS).json()


def get_universe_uuid_by_name(customer_uuid: str, universe_name: str) -> str:
    """
    Helper function to return the universeUUID of the Universe from a given friendly name.

    :param customer_uuid: str - the Customer UUID
    :param universe_name: str - the Universe's friendly name
    :return: str - the Universe's UUID
    :raises RuntimeError: if the Universe is not found
    """
    universe = next(iter(_get_universe_by_name(customer_uuid, universe_name)), None)
    if universe is None:
        raise RuntimeError(f"ERROR: failed to find a universe '{universe_name}' by name")
    else:
        return universe['universeUUID']


def get_database_namespaces(customer_uuid: str, universe_uuid: str, table_type='PGSQL_TABLE_TYPE') -> list:
    """
    Returns a list of database "namespaces" (database names) filtered by a given type. For xCluster DR this will be
    PGSQL_TABLE_TYPE (which is the default).

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/bc7e19ff7baec-list-all-namespaces
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/f0d617b52337d-namespace-info-resp

    :param customer_uuid: str - the Customer UUID
    :param universe_uuid: str - the Universe UUID
    :param table_type: str - the type of namespaces to return (e.g. YQL_TABLE_TYPE, REDIS_TABLE_TYPE, PGSQL_TABLE_TYPE,
     TRANSACTION_STATUS_TABLE_TYPE).
    :return: json array of NamespaceInfoResp
    """
    response = requests.get(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/universes/{universe_uuid}/namespaces",
                            headers=API_HEADERS).json()
    return list(filter(lambda db: db['tableType'] == table_type, response))


def pause_xcluster_config(customer_uuid: str, xcluster_config_uuid: str):
    """
    Pauses the underlying xCluster replication.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/3d17ffa45a16e-edit-xcluster-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param xcluster_config_uuid: str - the xCluster config UUID to pause
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    xcluster_replication_edit_form_data = {
        "status": "Paused"
    }
    return requests.put(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_config_uuid}",
                        json=xcluster_replication_edit_form_data, headers=API_HEADERS).json()


def resume_xcluster_config(customer_uuid: str, xcluster_config_uuid: str):
    """
    Resumes the underlying xCluster replication.

    See also:
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/3d17ffa45a16e-edit-xcluster-config
     - https://api-docs.yugabyte.com/docs/yugabyte-platform/64d854c13e51b-ybp-task

    :param customer_uuid: str - the Customer UUID
    :param xcluster_config_uuid: str - the xCluster config UUID to resume
    :return: json of YBPTask (it may be passed to wait_for_task)
    """
    xcluster_replication_edit_form_data = {
        "status": "Running"
    }
    return requests.put(url=f"{YBA_URL}/api/v1/customers/{customer_uuid}/xcluster_configs/{xcluster_config_uuid}",
                        json=xcluster_replication_edit_form_data, headers=API_HEADERS).json()


def get_source_xcluster_dr_config(customer_uuid: str, source_universe_name: str):
    """
    For a given source Universe returns the xCluster DR configuration associated with it.

    :param customer_uuid: str - the Customer UUID
    :param source_universe_name: str - the friendly name of the source Universe
    :return: json of DrConfig
    :raises RuntimeError: if the source Universe does not exist or does not have a DR config
    """
    get_source_universe_response = _get_universe_by_name(customer_uuid, source_universe_name)
    source_universe_details = next(iter(get_source_universe_response), None)
    if source_universe_details is None:
        raise RuntimeError(f"ERROR: the universe '{source_universe_name}' was not found.")
    else:
        dr_config_source_uuid = next(iter(source_universe_details['drConfigUuidsAsSource']), None)
        if dr_config_source_uuid is None:
            raise RuntimeError(f"ERROR: the universe '{source_universe_name}' does not have a DR config.")
        else:
            return _get_xcluster_dr_configs(customer_uuid, dr_config_source_uuid)


def create_xcluster_dr(customer_uuid: str, source_universe_name: str, target_universe_name: str, db_names=None,
                       dry_run=False):
    """
    Creates a new xCluster DR configuration for a given source and target Universe.

    :param customer_uuid: str - the Customer UUID
    :param source_universe_name: str - the friendly name of the source Universe
    :param target_universe_name: str - the friendly name of the target Universe
    :param db_names: set<str> - a set of YSQL database names to include in replication
    :param dry_run: bool - whether to perform a dry run
    :return: str - the xCluster DR config UUID created
    :raises RuntimeError: if the Universes or Storage Config are not found or if the Universe already has a DR config
    """
    storage_configs = _get_configs_by_type(customer_uuid, 'STORAGE')
    # pprint(storage_configs)
    if len(storage_configs) < 1:
        raise RuntimeError('WARN: no storage configs found, at least one is required for xCluster DR setup!')

    storage_config_uuid = storage_configs[0]['configUUID']  # todo how do we select this? we should pass in name
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

    create_dr_response = _create_dr_config(customer_uuid, storage_config_uuid, source_universe_uuid,
                                           target_universe_uuid, dbs_list_uuids, dry_run=dry_run)
    wait_for_task(customer_uuid, create_dr_response, 'Create xCluster DR')

    dr_config_uuid = create_dr_response['resourceUUID']
    print(f"SUCCESS: created disaster-recovery config {dr_config_uuid}")
    return dr_config_uuid


def delete_xcluster_dr(customer_uuid, source_universe_name) -> str:
    """
    Delete an xCluster DR configuration.
    This method can be externalized as it simplifies use of the underlying YBA API.

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :return:
    """
    get_universe_response = _get_universe_by_name(customer_uuid, source_universe_name)
    universe_details = next(iter(get_universe_response), None)
    if universe_details is None:
        raise RuntimeError(f"ERROR: source universe '{source_universe_name}' was not found!")

    dr_config_source_uuid = next(iter(universe_details["drConfigUuidsAsSource"]), None)
    if dr_config_source_uuid is None:
        raise RuntimeError(
            f"ERROR: source universe '{source_universe_name}' does not have a disaster-recovery config!")

    response = _delete_xcluster_dr_config(customer_uuid, dr_config_source_uuid)
    dr_config_uuid = wait_for_task(customer_uuid, response, "Delete xCluster DR")
    print(f"SUCCESS: deleted disaster-recovery config '{response['resourceUUID']}'.")
    return dr_config_uuid


def get_xcluster_dr_available_tables(customer_uuid: str, universe_name: str) -> list:
    """
    For a given universe name, returns a list of database tables not already included in the current xcluster dr config.
    These are the tables that can be added to the configuration. Ideally, these should have sizeBytes = 0 or including
    it will trigger a full backup/restore of the existing database (this will slow the process down).

    :param customer_uuid: str - the customer uuid.
    :param universe_name: str - the name of the universe.
    :return: list<str> - a list of database table ids not already included in the current xCluster DR config.
    """
    universe_uuid = get_universe_uuid_by_name(customer_uuid, universe_name)
    all_tables_list = _get_all_ysql_tables_list(customer_uuid, universe_uuid)
    # pprint(all_tables_list)

    # get the current xcluster dr list of included table ids (this is a child entry of the dr config)
    _xcluster_dr_existing_tables_id = get_source_xcluster_dr_config(customer_uuid, universe_name)['tables']
    # pprint(_xcluster_dr_existing_tables_id)

    # filter out any tables whose ids are not already in the current xcluster dr config
    available_tables_list = [t for t in all_tables_list if t['tableID'] not in _xcluster_dr_existing_tables_id]
    # pprint(filter_list)

    # TODO this list could also be compared to the target cluster to filter down tables not in both?

    return available_tables_list


def add_tables_to_xcluster_dr(customer_uuid: str, source_universe_name: str, add_tables_ids: set) -> str:
    """
    Adds a set of tables to replication in an existing xCluster DR config.

    See also: https://api-docs.yugabyte.com/docs/yugabyte-platform/branches/2.20/570cb66189f0d-set-tables-in-disaster-recovery-config

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :param add_tables_ids: set<str> - a set of table ids to add to replication
    :return: str - a resource uuid
    :raises RuntimeError: if no tables could be found to add to the xCluster DR config
    """
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
    return wait_for_task(customer_uuid, resp, "Add YSQL Tables")


def remove_tables_from_xcluster_dr(customer_uuid: str, source_universe_name: str, remove_tables_ids: set) -> str:
    """
    Removes a set of tables from replication from an existing xCluster DR config.

    This call should be made BEFORE performing a DROP TABLE operation.  Once the table(s) are removed from replication
    drop the table(s) from the replica first and then from the primary.

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :param remove_tables_ids: set<str> - the set of tables to remove
    :return: resource_uuid: str - the uuid of the resource being removed
    """
    xcluster_dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(xcluster_dr_config)
    xcluster_dr_uuid = xcluster_dr_config['uuid']
    storage_config_uuid = xcluster_dr_config['bootstrapParams']['backupRequestParams']['storageConfigUUID']

    filtered_dr_tables_list = list(filter(lambda t: t not in remove_tables_ids, xcluster_dr_config['tables']))
    # pprint(filtered_dr_tables_list)

    if len(filtered_dr_tables_list) == len(xcluster_dr_config['tables']):
        raise RuntimeError(f"ERROR: no table(s) can be removed from the xCluster DR config!")

    resp = _set_tables_in_dr_config(customer_uuid, xcluster_dr_uuid, storage_config_uuid, filtered_dr_tables_list)
    return wait_for_task(customer_uuid, resp, "Removing YSQL Tables from xCluster DR")


def perform_xcluster_dr_switchover(customer_uuid: str, source_universe_name: str) -> str:
    """
    Performs an xCluster DR switchover (aka a planned switchover).  This effectively changes the direction
    of the xCluster replication with zero RPO.

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :return: resource_uuid: str - the uuid of the resource being removed
    """
    dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(dr_config)
    dr_config_uuid = dr_config['uuid']
    primary_universe_uuid = dr_config['primaryUniverseUuid']
    dr_replica_universe_uuid = dr_config['drReplicaUniverseUuid']

    resp = _switchover_xcluster_dr(customer_uuid, dr_config_uuid, primary_universe_uuid, dr_replica_universe_uuid)
    return wait_for_task(customer_uuid, resp, "Switchover XCluster DR")


def perform_xcluster_dr_failover(customer_uuid: str, source_universe_name: str) -> str:
    """
    Performs an xCluster DR failover (aka an unplanned switchover).  This promotes the DR replica to be the Primary.
    This operation has a small, but non-zero RPO.

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :return: resource_uuid: str - the uuid of the resource being failed over to?
    """
    dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(dr_config)
    dr_config_uuid = dr_config['uuid']
    primary_universe_uuid = dr_config['primaryUniverseUuid']
    dr_replica_universe_uuid = dr_config['drReplicaUniverseUuid']

    xcluster_dr_safetimes = _get_xcluster_dr_safetime(customer_uuid, dr_config_uuid)
    # pprint(xcluster_dr_safetimes)
    safetime_epoch_map = {
        entry["namespaceId"]: entry["safetimeEpochUs"] for entry in xcluster_dr_safetimes['safetimes']
    }
    # pprint(safetime_epoch_map)

    resp = _failover_xcluster_dr(customer_uuid, dr_config_uuid, primary_universe_uuid, dr_replica_universe_uuid,
                                 safetime_epoch_map)
    return wait_for_task(customer_uuid, resp, "Failover XCluster DR")


def perform_xcluster_dr_repair(customer_uuid: str, source_universe_name: str) -> str:
    """
    Performs an xCluster DR Repair after an unplanned fail-over.

    This operation currently assumes that the users intent is to reuse the original Primary (the failed) cluster as the
    new DR Replica. This operation will trigger a full bootstrap of the current Primary and restores it to the old
    Primary. This process will take longer based on the size of the database(s) being restored.

    :param customer_uuid: str - the customer uuid
    :param source_universe_name: str - the name of the source universe
    :return: resource_uuid: str - the uuid of the resource being repaired
    """
    dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(dr_config)
    dr_config_uuid = dr_config['uuid']

    resp = _restart_xcluster_dr_config(customer_uuid, dr_config_uuid)
    return wait_for_task(customer_uuid, resp, "Repair XCluster DR")


def perform_xcluster_dr_sync(customer_uuid: str, source_universe_name: str) -> str:
    """
    Synchronizes the xCluster DR config with any underlying changes made outside the YBA UI.

    :param customer_uuid: str - the customer UUID
    :param source_universe_name: str - the name of the source Universe
    :return:
    """
    dr_config = get_source_xcluster_dr_config(customer_uuid, source_universe_name)
    # pprint(dr_config)
    dr_config_uuid = dr_config['uuid']

    resp = _sync_xcluster_dr_config(customer_uuid, dr_config_uuid)
    return wait_for_task(customer_uuid, resp, "Synchronize XCluster DR")


def testing():
    """
    Testing Block:
    Use this section to test out the various operations using the configurations provided (or override them for
    yourself).
    """
    xcluster_east = 'ssherwood-xcluster-east'
    xcluster_central = 'ssherwood-xcluster-central'
    include_database_names = {'yugabyte', 'yugabyte2'}
    test_task_uuid = 'aac2ded4-0b54-4818-affd-fc8ca40c69b1'
    add_list = {'00004000000030008000000000004002'}
    remove_list = {'00004000000030008000000000004002'}

    try:
        user_session = _get_session_info()
        customer_uuid = user_session['customerUUID']
        east_universe_uuid = get_universe_uuid_by_name(customer_uuid, xcluster_east)
        central_universe_uuid = get_universe_uuid_by_name(customer_uuid, xcluster_central)

        execute_option = 'resume_xcluster_config'
        match execute_option:
            case 'create_xcluster_dr':
                create_xcluster_dr(customer_uuid, xcluster_east, xcluster_central, include_database_names)
            case 'get_source_xcluster_dr_config':
                pprint(get_source_xcluster_dr_config(customer_uuid, xcluster_east))
            case 'delete_xcluster_dr':
                delete_xcluster_dr(customer_uuid, xcluster_east)
            case 'pause_xcluster_config':
                dr_config = get_source_xcluster_dr_config(customer_uuid, xcluster_east)
                resp = pause_xcluster_config(customer_uuid, dr_config['xclusterConfigUuid'])
                wait_for_task(customer_uuid, resp, "Pause XCluster")
            case 'resume_xcluster_config':
                dr_config = get_source_xcluster_dr_config(customer_uuid, xcluster_east)
                resp = resume_xcluster_config(customer_uuid, dr_config['xclusterConfigUuid'])
                wait_for_task(customer_uuid, resp, "Resume XCluster")
            case 'get_database_namespaces':
                pprint(get_database_namespaces(customer_uuid, east_universe_uuid))
            case 'get_xcluster_dr_available_tables':
                pprint(get_xcluster_dr_available_tables(customer_uuid, xcluster_east))
            case 'add_tables_to_xcluster_dr':
                pprint(add_tables_to_xcluster_dr(customer_uuid, xcluster_east, add_list))
            case 'remove_tables_from_xcluster_dr':
                pprint(remove_tables_from_xcluster_dr(customer_uuid, xcluster_east, remove_list))
            case 'perform_xcluster_dr_switchover':
                pprint(perform_xcluster_dr_switchover(customer_uuid, xcluster_central))
            case 'perform_xcluster_dr_failover':
                pprint(perform_xcluster_dr_failover(customer_uuid, xcluster_east))
            case 'perform_xcluster_dr_repair':
                pprint(perform_xcluster_dr_repair(customer_uuid, xcluster_central))
            case 'perform_xcluster_dr_sync':
                pprint(perform_xcluster_dr_sync(customer_uuid, xcluster_east))
            case '_get_task_status':
                pprint(_get_task_status(customer_uuid, test_task_uuid))
            case '_get_xcluster_configs':
                pprint(_get_xcluster_configs(customer_uuid, '123'))
            case '_get_all_ysql_tables_list':
                pprint(_get_all_ysql_tables_list(customer_uuid, east_universe_uuid))
            case '_validate_dr_replica_tables':
                tables_list = [{'colocated': False,
                                'isIndexTable': False,
                                'keySpace': 'yugabyte2',
                                'pgSchemaName': 'public2',
                                'relationType': 'USER_TABLE_RELATION',
                                'sizeBytes': 0.0,
                                'tableID': '00004000000030008000000000004002',
                                'tableName': 'foo',
                                'tableType': 'PGSQL_TABLE_TYPE',
                                'tableUUID': '00004000-0000-3000-8000-000000004002',
                                'walSizeBytes': 6291456.0}]
                _validate_dr_replica_tables(customer_uuid, central_universe_uuid, tables_list)
            case '_get_xcluster_dr_safetime':
                dr_config = get_source_xcluster_dr_config(customer_uuid, xcluster_east)
                pprint(_get_xcluster_dr_safetime(customer_uuid, dr_config['uuid']))
    except Exception as ex:
        print(ex)


testing()
