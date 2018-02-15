import io
import logging
import os
import tarfile
from contextlib import contextmanager

from queue import Empty

import time
import multiprocessing
# TODO: Investigate this, seems to reqire fork when using a UNIX socket and spawn if using TCP.
mp_ctx = multiprocessing.get_context("fork")

logger = logging.getLogger(__name__)


class Pool:
    CONTAINER_STOP_TIMEOUT = 0
    # How long to wait between issuing reset commands to containers, should be less than the actual timeout interval
    # within the container to ensure some margin for error
    CONTAINER_RESETTER_INTERVAL_SECONDS = 3

    def __init__(self, client, image_suffix, min_pool_size, min_available, required_packages, base_image):
        self.client = client
        self.image_name = f"sandbox-{image_suffix}"
        self.min_pool_size = min_pool_size
        self.min_available = min_available
        self.required_packages = required_packages
        self.base_image = base_image

        manager = mp_ctx.Manager()
        self._available_workers = manager.list()
        self._running_workers = manager.dict()  # TODO: Is there a better structure than this than a dict?
        self.pool_manager_process = None
        self.pool_manager_queue = None
        self.container_timeout_resetter_process = None
        self.container_timeout_resetter_queue = None

    def build_image(self):
        # TODO: Validate that this installs the specific package version!

        # The container should run "tail -f /dev/null" as this will block and keep the container running, this also
        # doesn't require a TTY or STDIN to be open unlike other alternatives such as by running sh or cat
        container_tools_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "container_tools")

        dockerfile_commands = [
            f"FROM {self.base_image}",
            "COPY container_tools/ /container_tools",
            "RUN chmod -R +x /container_tools/sbin/",
            "CMD [ \"/container_tools/sbin/container_timeout.py\", \"timeout\" ]"
        ]
        for package in self.required_packages:
            dockerfile_commands.append(f"RUN pip install --no-cache-dir {package}")

        print(dockerfile_commands)

        # Docker library wants a file, so convert our dockerfile array to a string then wrap in a file-like object
        dockerfile_string = "\n".join(dockerfile_commands)

        # Create in memory tar archive containing the build context
        # TODO: Try and get this working with an in memory fileobj instead of a temp file
        with tarfile.open(name="/tmp/build_context.tar", mode="w") as tar:
            dockerfile_info = tarfile.TarInfo("dockerfile")
            dockerfile_info.size = len(dockerfile_string)
            tar.addfile(dockerfile_info, io.BytesIO(dockerfile_string.encode("UTF-8")))
            tar.add(container_tools_directory, arcname="container_tools")

        with open("/tmp/build_context.tar", "rb") as f:
            logger.info("Building image")
            self.client.images.build(fileobj=f, custom_context=True, tag=self.image_name)
        logger.info("Image created")

    def start_container_timeout_resetter(self):
        self.container_timeout_resetter_queue = mp_ctx.Queue()
        self.container_timeout_resetter_process = mp_ctx.Process(target=self._container_timeout_resetter,
                                            args=((self.container_timeout_resetter_queue),))
        self.container_timeout_resetter_process.start()

    def stop_container_timeout_resetter(self):
        logger.info("Shutting down container timeout resetter")
        self.container_timeout_resetter_queue.put("STOP")
        self.container_timeout_resetter_queue.join()

    def start_pool_manager(self):
        self.pool_manager_queue = mp_ctx.Queue()
        self.pool_manager_process = mp_ctx.Process(target=self._container_respawner_process, args=((self.pool_manager_queue),))
        self.pool_manager_process.start()

    def stop_pool_manager(self):
        logger.info("Shutting down pool manager")
        self.pool_manager_queue.put("STOP")
        self.pool_manager_process.join()

    @contextmanager
    def get_container(self):
        try:
            container_id = self._available_workers.pop(0)
            container = self.client.containers.get(container_id)
        except IndexError:
            logger.warning("No containers in pool when container requested, spawning container now")
            container = self._start_container()
        self._running_workers[container.id] = container.id
        yield container
        # We are now finished with the container so stop it
        del self._running_workers[container.id]
        container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _start_container(self):
        container = self.client.containers.run(self.image_name, auto_remove=True, detach=True, network_disabled=True)
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
        while True:
            self._ensure_minimum_containers()
            try:
                msg = queue.get(block=True, timeout=0.5)  # TODO: Make timeout configurable
                if msg == "STOP":
                    self._shutdown_all_containers()
                    break
            except Empty:
                pass  # We don't care if the queue is empty

    def _container_timeout_resetter(self, queue):

        while True:
            next_reset_timestamp = int(time.time()) + self.CONTAINER_RESETTER_INTERVAL_SECONDS

            container_ids_to_reset = self._available_workers + list(self._running_workers.values())
            for container_id in container_ids_to_reset:
                self.client.containers.get(container_id).exec_run("/container_tools/sbin/container_timeout.py reset")

            try:
                # Work out how long to wait until the time matches next_reset_timestamp
                timeout_seconds = (next_reset_timestamp - int(time.time())) or 0
                msg = queue.get(block=True, timeout=timeout_seconds)
                if msg == "STOP":
                    break
            except Empty:
                pass  # We don't care if the queue is empty
