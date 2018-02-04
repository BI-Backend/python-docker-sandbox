import io
import logging
import atexit
from contextlib import contextmanager
from collections import deque

logger = logging.getLogger(__name__)


class Pool:
    CONTAINER_STOP_TIMEOUT = 0

    def __init__(self, client, image_suffix, min_pool_size, min_available, required_packages, base_image):
        self.client = client
        self.image_name = f"sandbox-{image_suffix}"
        self.min_pool_size = min_pool_size
        self.min_available = min_available
        self.required_packages = required_packages
        self.base_image = base_image

        self.available_workers = deque([])
        self.running_workers = {}

        atexit.register(self._shutdown_all_containers)

    def build_image(self):
        # TODO: Validate that this installs the specific package version!

        # The container should run "tail -f /dev/null" as this will block and keep the container running, this also
        # doesn't require a TTY or STDIN to be open unlike other alternatives such as by running sh or cat
        dockerfile_commands = [f"FROM {self.base_image}", "CMD [ \"tail\", \"-f\", \"/dev/null\" ]"]
        for package in self.required_packages:
            dockerfile_commands.append(f"RUN pip install --no-cache-dir {package}")

        # Docker library wants a file, so convert our dockerfile array to a string then wrap in a file-like object
        dockerfile = io.BytesIO("\n".join(dockerfile_commands).encode("UTF-8"))

        logger.info("Building image")
        self.client.images.build(fileobj=dockerfile, tag=self.image_name)
        logger.info("Image created")

    @contextmanager
    def get_container(self):
        # TODO: If pool is empty (could happen if _ensure_minimum_containers hasn't yet run), start a new container
        container = self.available_workers.popleft()
        self.running_workers[container.id] = container
        yield container
        # We are now finished with the container so stop it
        del self.running_workers[container.id]
        container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _start_container(self):
        container = self.client.containers.run(self.image_name, auto_remove=True, detach=True)
        return container

    def _ensure_minimum_containers(self):
        available_workers = len(self.available_workers)
        total_workers = available_workers + len(self.running_workers)

        workers_to_start = 0
        available_difference = self.min_available - available_workers
        if available_difference > 0:
            workers_to_start = available_difference

        pool_size_difference = self.min_pool_size - total_workers
        if pool_size_difference > 0:
            workers_to_start = max(workers_to_start, pool_size_difference)

        if workers_to_start > 0:
            logger.info(f"Starting {workers_to_start} workers")

        for i in range(0, workers_to_start):
            container = self._start_container()
            self.available_workers.append(container)

        logger.info(f"{len(self.available_workers) + len(self.running_workers)} workers are currently running")

    def _shutdown_all_containers(self):
        # TODO: Stop any threads that may change lists of available/running workers!

        containers_to_stop = list(self.available_workers) + list(self.running_workers.values())

        logger.info(f"Shutting down {len(containers_to_stop)} containers")

        for container in containers_to_stop:
            logger.info(f"Shutting down container {container.id}")
            container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)
