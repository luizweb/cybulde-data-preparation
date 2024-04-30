from cybulde.config_schemas.data_processing_config_schema import DataProcessingConfig
from cybulde.utils.config_utils import get_config
from cybulde.utils.gcp_utils import access_secret_version
from cybulde.utils.data_utils import get_raw_data_with_version

from hydra.utils import instantiate


@get_config(config_path="../configs", config_name="data_processing_config")
def process_data(config: DataProcessingConfig) -> None:
    #print(config)
    #print(60 * "*")
    #from omegaconf import OmegaConf
    #print(OmegaConf.to_yaml(config))
    #return

    #my_test_secret = access_secret_version("luizweb", "test-secret", "latest")
    #print(f"My test secret: {my_test_secret}")

    #github_token = access_secret_version("luizweb", "cybulde-data-github-access-token", "latest")
    #print(f"Github token: {github_token}")

    #version = "v2"
    #data_local_save_dir = "./data/raw"
    #dvc_remote_repo = "https://github.com/luizweb/cybulde-data.git"
    #dvc_data_folder = "data/raw"
    #github_user_name = "luizweb"

    #github_access_token = access_secret_version("luizweb", "cybulde-data-github-access-token", "latest")
    github_access_token = access_secret_version(config.infrastructure.project_id, config.github_access_token_secret_id)


    get_raw_data_with_version(
        version=config.version,
        data_local_save_dir=config.data_local_save_dir,
        dvc_remote_repo=config.dvc_remote_repo,
        dvc_data_folder=config.dvc_data_folder,
        github_user_name=config.github_user_name,
        github_access_token=github_access_token,
    )

    dataset_reader_manager = instantiate(config.dataset_reader_manager)
    df = dataset_reader_manager.read_data()

    print(df.head(10))
    print(df["dataset_name"].unique().compute())



if __name__ == "__main__":
    process_data()  # type: ignore
