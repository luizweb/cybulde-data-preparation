from cybulde.utils.utils import run_shell_command
from shutil import rmtree


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


