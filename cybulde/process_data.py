from hydra.utils import instantiate
from pathlib import Path
import os

from dask.distributed import Client
import dask.dataframe as dd

from cybulde.config_schemas.data_processing.dataset_cleaners_schema import DatasetCleanerManagerConfig
from cybulde.config_schemas.data_processing_config_schema import DataProcessingConfig
from cybulde.utils.config_utils import get_config, get_pickle_config
from cybulde.utils.data_utils import get_raw_data_with_version
from cybulde.utils.gcp_utils import access_secret_version
from cybulde.utils.utils import get_logger



def process_raw_data(df_partition: dd.core.DataFrame, dataset_cleaner_manager: DatasetCleanerManagerConfig) -> dd.core.Series:
    processed_partition: dd.core.Series = df_partition["text"].apply(dataset_cleaner_manager)
    return processed_partition


@get_pickle_config(config_path="cybulde/configs/automatically_generated", config_name="data_processing_config")
#@get_config(config_path="../configs", config_name="data_processing_config")
def process_data(config: DataProcessingConfig) -> None:
    print(config)
    #print(60 * "#")
    #from omegaconf import OmegaConf
    #print(OmegaConf.to_yaml(config))
    #print(60 * "#")
    return

    # my_test_secret = access_secret_version("luizweb", "test-secret", "latest")
    # print(f"My test secret: {my_test_secret}")

    # github_token = access_secret_version("luizweb", "cybulde-data-github-access-token", "latest")
    # print(f"Github token: {github_token}")

    # version = "v2"
    # data_local_save_dir = "./data/raw"
    # dvc_remote_repo = "https://github.com/luizweb/cybulde-data.git"
    # dvc_data_folder = "data/raw"
    # github_user_name = "luizweb"

    # github_access_token = access_secret_version("luizweb", "cybulde-data-github-access-token", "latest")
    
    logger = get_logger(Path(__file__).name)
    logger.info("Processing raw data...")
    
    processed_data_save_dir = config.processed_data_save_dir

    # Dask
    cluster = instantiate(config.dask_cluster)
    client = Client(cluster)

    try:

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
        dataset_cleaner_manager = instantiate(config.dataset_cleaner_manager)

        # df = dataset_reader_manager.read_data()
        df = dataset_reader_manager.read_data(config.dask_cluster.n_workers)

        print(60 * "#")
        print(f"{df.npartitions=}")
        print(60 * "#")
        #exit(0)


        #sample_df = df.sample(n=5)

        #for _, row in sample_df.iterrows():
        #    text = row["text"]
        #    cleaned_text = dataset_cleaner_manager(text)

        #    print(60 * "#")
        #    print(f"Text: {text}")
        #    print(f"Cleaned text: {cleaned_text}")
        #    print(60 * "#")

        # print(df.head(10))
        # print(df["dataset_name"].unique().compute())

        logger.info("Cleanning data...")
        df = df.assign(cleaned_text=df.map_partitions(process_raw_data, dataset_cleaner_manager=dataset_cleaner_manager, meta=("text","object")))
        df = df.compute()

        train_parquet_path = os.path.join(processed_data_save_dir, "train.parquet")
        dev_parquet_path = os.path.join(processed_data_save_dir, "dev.parquet")
        test_parquet_path = os.path.join(processed_data_save_dir, "test.parquet")

        train_df = df[df["split"] == "train"].to_parquet(train_parquet_path)
        dev_df = df[df["split"] == "dev"].to_parquet(dev_parquet_path)
        test_df = df[df["split"] == "test"].to_parquet(test_parquet_path)

        logger.info("Data processing finished!")

    finally:
        logger.info("Closing dask cluster...")
        client.close()
        cluster.close()


if __name__ == "__main__":
    process_data()
