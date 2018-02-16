import io
import logging
import os
import tarfile
from contextlib import contextmanager

from queue import Empty

import time
import multiprocessing
# TODO: Investigate this, seems to reqire fork when using a UNIX socket and spawn if using TCP.
import docker.errors

mp_ctx = multiprocessing.get_context("fork")

logger = logging.getLogger(__name__)


class Pool:
    CONTAINER_STOP_TIMEOUT = 0
    # How long to wait between issuing reset commands to containers, should be less than the actual timeout interval
    # within the container to ensure some margin for error
    CONTAINER_RESETTER_INTERVAL_SECONDS = 3
    DEAD_CONTAINER_CLEANUP_INTERVAL_SECONDS = 5

    def __init__(self, client, image_suffix, min_pool_size, min_available, required_packages, base_image):
        """
        Initialises a pool object
        :param client: - An instance of docker.DockerClient
        :param image_suffix: - The suffix to apply to the created image name, must be unique to this pool instance
        :param min_pool_size: - The minimum number of workers that must be either running or available at a given time
        :param min_available: - The minimum of workers that must be available for use at a given time
        :param required_packages: - A list of packages to be installed, each entry should be in a format understood by
                                    pip (either a package name on its own or with version restrictions)
        :param base_image: - The name of a docker image to base the custom image on.  Must include python and pip.
        """
        self.client = client
        self.image_name = f"sandbox-{image_suffix}"
        self.min_pool_size = min_pool_size
        self.min_available = min_available
        self.required_packages = required_packages
        self.base_image = base_image

        manager = mp_ctx.Manager()
        self._available_workers = manager.list()
        self._running_workers = manager.list()
        self.pool_manager_process = None
        self.pool_manager_queue = None
        self.container_timeout_resetter_process = None
        self.container_timeout_resetter_queue = None
        self.dead_container_cleanup_process = None
        self.dead_container_cleanup_queue = None

    def build_image(self):
        """
        Builds a docker image based on the requirements specified at pool initialisation
        :return:
        """
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
        """
        Starts _container_timeout_resetter_process running in the background
        :return:
        """
        self.container_timeout_resetter_queue = mp_ctx.Queue()
        self.container_timeout_resetter_process = mp_ctx.Process(target=self._container_timeout_resetter_process,
                                                                 args=((self.container_timeout_resetter_queue),))
        self.container_timeout_resetter_process.start()

    def stop_container_timeout_resetter(self):
        """
        Issues a stop command to _container_timeout_resetter_process and waits for it to exit
        :return:
        """
        logger.info("Shutting down container timeout resetter")
        self.container_timeout_resetter_queue.put("STOP")
        self.container_timeout_resetter_queue.join()

    def start_pool_manager(self):
        """
        Starts _container_respawner_process running in the background
        :return:
        """
        self.pool_manager_queue = mp_ctx.Queue()
        self.pool_manager_process = mp_ctx.Process(target=self._container_respawner_process,
                                                   args=((self.pool_manager_queue),))
        self.pool_manager_process.start()

    def stop_pool_manager(self):
        """
        Issues a stop command to _container_respawner_process and waits for it to exit
        :return:
        """
        logger.info("Shutting down pool manager")
        self.pool_manager_queue.put("STOP")
        self.pool_manager_process.join()

    def start_dead_container_cleanup_process(self):
        """
        Starts _dead_container_cleanup_process running in the background
        :return:
        """
        self.dead_container_cleanup_queue = mp_ctx.Queue()
        self.dead_container_cleanup_process = mp_ctx.Process(target=self._dead_container_cleanup_process,
                                                   args=((self.dead_container_cleanup_queue),))
        self.dead_container_cleanup_process.start()

    def stop_dead_container_cleanup_process(self):
        """
        Issues a stop command to _dead_container_cleanup_process and waits for it to exit
        :return:
        """
        logger.info("Shutting down dead container cleanup process")
        self.dead_container_cleanup_queue.put("STOP")
        self.dead_container_cleanup_process.join()

    @contextmanager
    def get_container(self):
        """
        Removes a container from the list of available workers, adds it to the dict of running workers and then returns
        it. This acts as a context manager so when the context runtime is exited the container will be stopped and
        forgotten about so that a new, clean container can be spawned by the respawner process. Yields a container
        object.
        :return:
        """
        container = None
        container_found = False
        while not container_found:
            try:
                container_id = self._available_workers.pop(0)
                container = self.client.containers.get(container_id)
            except docker.errors.NotFound:
                logger.warning("Container ID retrieved from the pool was not running, attempting to get another")
                continue
            except IndexError:
                logger.warning("No containers in pool when container requested, spawning container now")
                container = self._start_container()
            container_found = True

        self._running_workers.append(container.id)
        yield container
        # We are now finished with the container so stop it
        self._running_workers.remove(container.id)
        container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _start_container(self):
        """
        Starts a new container in the background
        :return: - A container object
        """
        container = self.client.containers.run(self.image_name, auto_remove=True, detach=True, network_disabled=True)
        return container

    def _ensure_minimum_containers(self):
        """
        Checks the total number of containers based on the thresholds defined at pool initialisaion and if there are
        too few containers it will spawn the appropriate number of new ones.
        :return:
        """
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

        logger.debug(f"{len(self._available_workers) + len(self._running_workers)} workers are currently running")

    def _shutdown_all_containers(self):
        """
        Loops through all containers and shuts them down
        :return:
        """
        containers_to_stop = list(self._available_workers) + list(self._running_workers)

        logger.info(f"Shutting down {len(containers_to_stop)} containers")

        for container_id in containers_to_stop:
            container = self.client.containers.get(container_id)
            logger.info(f"Shutting down container {container.id}")
            container.stop(timeout=self.CONTAINER_STOP_TIMEOUT)

    def _container_respawner_process(self, queue):
        """
        This process runs at a regular interval and checks how many containers are currently running, if there is an
        insufficient number (based on the thresholds supplied when the pool was initialised) it will spawn additional
        containers.
        :param queue: - A multiprocessing queue, this is used to stop the process by sending the string "STOP"
        :return:
        """
        while True:
            self._ensure_minimum_containers()
            try:
                msg = queue.get(block=True, timeout=0.5)  # TODO: Make timeout configurable
                if msg == "STOP":
                    self._shutdown_all_containers()
                    break
            except Empty:
                pass  # We don't care if the queue is empty

    def _container_timeout_resetter_process(self, queue):
        """
        This process regularly executes the container timeout reset command on all containers to ensure that they do
        not timeout.  It will skip any containers who have an ID stored in the pool but do not exist within docker (e.g.
        if the container has failed or was stopped by and external process)
        :param queue: - A multiprocessing queue, this is used to stop the process by sending the string "STOP"
        :return:
        """
        while True:
            next_reset_timestamp = int(time.time()) + self.CONTAINER_RESETTER_INTERVAL_SECONDS

            container_ids_to_reset = self._available_workers + list(self._running_workers)
            for container_id in container_ids_to_reset:
                try:
                    container = self.client.containers.get(container_id)
                except docker.errors.NotFound:
                    logger.warning("Attempted to reset timeout on a container from pool but container did not exist,"
                                   " skipping...")
                    continue
                container.exec_run("/container_tools/sbin/container_timeout.py reset")

            try:
                # Work out how long to wait until the time matches next_reset_timestamp
                timeout_seconds = (next_reset_timestamp - int(time.time())) or 0
                msg = queue.get(block=True, timeout=timeout_seconds)
                if msg == "STOP":
                    break
            except Empty:
                pass  # We don't care if the queue is empty

    def _dead_container_cleanup_process(self, queue):
        """
        This process regularly checks that all of the container IDs stored in both the running workers and available
        workers lists are actually still running within docker, if not it will remove the process from the appropriate
        list.  This ensures that containers which have potentially crashed or otherwise failed are forgotten about so
        that they can be respawned by the respawner process.
        :param queue: - A multiprocessing queue, this is used to stop the process by sending the string "STOP"
        :return:
        """
        while True:
            all_container_ids = self._available_workers + list(self._running_workers)
            for container_id in all_container_ids:
                container_dead = False
                try:
                    self.client.containers.get(container_id)
                except docker.errors.NotFound:
                    logger.warning("Dead container found, forgetting...")
                    container_dead = True

                if container_dead:
                    try:
                        self._available_workers.remove(container_id)
                        self._running_workers.remove(container_id)
                    except (ValueError, KeyError):
                        pass

            try:
                msg = queue.get(block=True, timeout=self.DEAD_CONTAINER_CLEANUP_INTERVAL_SECONDS)
                if msg == "STOP":
                    break
            except Empty:
                pass  # We don't care if the queue is empty
