import docker
import logging
from .connectors import SQLServerConnector
from .dates_updater import DatesUpdater

logger = logging.getLogger(__name__)

def get_docker_client() -> docker.DockerClient | None:
    docker_client = None
    try:
        docker_client = docker.from_env()
        logger.info("Docker client initialized.")
    except docker.errors.DockerException as e:
        logger.error(f"Docker client couldn't be initialized: {e}.\nEnsure Docker Dekstop is running.")
    except docker.errors.APIError as e:
        logger.error(f"Docker error occurred: {e}")

    return docker_client

def start_container(
        docker_client: docker.DockerClient,
        container_name: str
    ) -> None:

    container = None
    try:
        container = docker_client.containers.get(container_name)
        
        if container.status != "running":
            container.start()
            logger.info(f"Container '{container_name}' started.")
        else:
            logger.info(f"Container '{container_name}' is already running.")
    
    except docker.errors.NotFound:
        logger.error(f"Container '{container_name}' not found.")
    except docker.errors.APIError as e:
        logger.error(f"Docker error occurred: {e}")

    return container

if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    docker_client = get_docker_client()
    if not docker_client:
        exit()

    sql_server_container = start_container(docker_client, 'sqlserver2022')

    sql_server = SQLServerConnector()
    dates_updater = DatesUpdater(sql_server)
    dates_updater.update_dates()
