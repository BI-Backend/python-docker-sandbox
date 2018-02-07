import io
import logging
import time
from contextlib import contextmanager
from collections import deque

from multiprocessing import Process, Queue, Manager
from queue import Empty

logger = logging.getLogger(__name__)
manager = Manager()


class Pool:
    CONTAINER_STOP_TIMEOUT = 0

    def __init__(self, client, image_suffix, min_pool_size, min_available, required_packages, base_image):
        self.client = client
        self.image_name = f"sandbox-{image_suffix}"
        self.min_pool_size = min_pool_size
        self.min_available = min_available
        self.required_packages = required_packages
        self.base_image = base_image

        self._available_workers = manager.list()
        self._running_workers = manager.dict()
        self.pool_manager_process = None
        self.pool_manager_queue = None

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

    def start_pool_manager(self):
        self.pool_manager_queue = Queue()
        self.pool_manager_process = Process(target=self._container_respawner_process, args=((self.pool_manager_queue),))
        self.pool_manager_process.start()

    def stop_pool_manager(self):
        logger.info("Shutting down pool manager")
        self.pool_manager_queue.put("STOP")
        self.pool_manager_process.join()

    # @contextmanager
    # def get_container(self):
    #     # TODO: If pool is empty (could happen if _ensure_minimum_containers hasn't yet run), start a new container
    #     container = self._available_workers.popleft()
    #     self._running_workers[container.id] = container
    #     yield container
    #     # We are now finished with the container so stop it
    #     del self._running_workers[container.id]
    #     container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _start_container(self):
        container = self.client.containers.run(self.image_name, auto_remove=True, detach=True)
        return container

    def _ensure_minimum_containers(self):
        available_workers = len(self._available_workers)
        total_workers = available_workers + len(self._running_workers)

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
            self._available_workers.append(container.id)

        logger.info(f"{len(self._available_workers) + len(self._running_workers)} workers are currently running")

    def _shutdown_all_containers(self):
        containers_to_stop = list(self._available_workers) + list(self._running_workers.values())

        logger.info(f"Shutting down {len(containers_to_stop)} containers")

        for container_id in containers_to_stop:
            container = self.client.containers.get(container_id)
            logger.info(f"Shutting down container {container.id}")
            container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _container_respawner_process(self, queue):
        try:
            while True:
                self._ensure_minimum_containers()
                try:
                    msg = queue.get(block=True, timeout=0.5)  # TODO: Make timeout configurable
                    if msg == "STOP":
                        logger.info("Pool manager received stop command, shutting down containers")
                        self._shutdown_all_containers()
                        break
                except Empty:
                    pass  # We don't care if the queue is empty
        except (KeyboardInterrupt, SystemExit):
            logger.info("Application exiting, shutting down containers")
            self._shutdown_all_containers()
