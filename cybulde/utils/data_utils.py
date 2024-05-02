from shutil import rmtree
from typing import Optional

import dask.dataframe as dd
import psutil
from cybulde.utils.utils import run_shell_command


def get_cmd_to_get_raw_data(
    version: str,
    data_local_save_dir: str,
    dvc_remote_repo: str,
    dvc_data_folder: str,
    github_user_name: str,
    github_access_token: str,
) -> str:
    """Get shell command to get raw data from DVC store

    Parameters
    ----------
    version : str
        The version to get
    data_local_save_dir : str
        The local directory where to save the data
    dvc_remote_repo : str
        The DVC remote repository that holds information about the data
    dvc_data_folder : str
        Location where the remote data is stored
    github_user_name : str
        The GitHub user name
    github_access_token : str
        The GitHub access token

    Returns
    -------
    str
        The shell command to get the raw data

    """
    """
    dvc_remote_repo = "https://github.com/luizweb/cybulde-data.git"
    we_want_this = "htts://<username>:<access_token>@github.com/luizweb/cybulde-data.git"
    """

    without_https = dvc_remote_repo.replace("https://", "")
    dvc_remote_repo = f"https://{github_user_name}:{github_access_token}@{without_https}"

    command = f"dvc get {dvc_remote_repo} {dvc_data_folder} --rev {version} -o {data_local_save_dir}"
    return command


def get_raw_data_with_version(
    version: str,
    data_local_save_dir: str,
    dvc_remote_repo: str,
    dvc_data_folder: str,
    github_user_name: str,
    github_access_token: str,
) -> None:
    rmtree(data_local_save_dir, ignore_errors=True)
    command = get_cmd_to_get_raw_data(
        version,
        data_local_save_dir,
        dvc_remote_repo,
        dvc_data_folder,
        github_user_name,
        github_access_token,
    )
    run_shell_command(command)


def get_nrof_partitions(
    df_memory_usage: int,
    nrof_workers: int,
    available_memory: Optional[float],
    min_partition_size: int,
    aimed_nrof_partitions_per_worker: int,
) -> int:
    if available_memory is None:
        available_memory = psutil.virtual_memory().available
    else:
        available_memory = available_memory * nrof_workers

    if df_memory_usage <= min_partition_size:
        return 1

    if df_memory_usage / nrof_workers <= min_partition_size:
        return round(df_memory_usage / min_partition_size)

    nrof_partitions_per_worker = 0
    required_memory = float("inf")

    while required_memory > available_memory:
        nrof_partitions_per_worker += 1
        required_memory = df_memory_usage / nrof_partitions_per_worker

    nrof_partitions = nrof_partitions_per_worker * nrof_workers

    while (df_memory_usage / (nrof_partitions + 1)) > min_partition_size and (
        nrof_partitions // nrof_workers
    ) < aimed_nrof_partitions_per_worker:
        nrof_partitions += 1

    return nrof_partitions




def repartition_dataframe(
    df: dd.core.DataFrame,
    nrof_workers: int,
    available_memory: Optional[float] = None,
    min_partition_size: int = 15 * 1024**2,
    aimed_nrof_partitions_per_worker: int = 10,
) -> dd.core.DataFrame:
    df_memory_usage = df.memory_usage(deep=True).sum().compute()
    nrof_partitions = get_nrof_partitions(
        df_memory_usage, nrof_workers, available_memory, min_partition_size, aimed_nrof_partitions_per_worker
    )
    partitioned_df: dd.core.DataFrame = df.repartition(npartitions=1).repartition(npartitions=nrof_partitions)  # type: ignore
    return partitioned_df
