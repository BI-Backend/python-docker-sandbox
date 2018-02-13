import io
import logging
import docker

from python_docker_sandbox.pool import Pool

logger = logging.getLogger(__name__)

class Sandbox:
    def __init__(self, base_url="unix:///var/run/docker.sock", client_cert=None, client_key=None, client_verify=None):
        self.base_url = base_url
        self.client_cert = client_cert
        self.client_key = client_key
        self.client_verify = client_verify
        self.pool = None

        url_type = base_url.split(":")[0].lower()
        if url_type == "unix":
            self.client = docker.DockerClient(base_url=base_url)
        elif url_type == "tcp":
            tls_config = docker.tls.TLSConfig(client_cert=(client_cert, client_key), verify=client_verify)
            self.client = docker.DockerClient(base_url=base_url, tls=tls_config)
        else:
            raise ValueError("Invalid base_url")

    def init_pool(self, image_suffix, min_pool_size=5, min_available=2, required_packages=[],
                  base_image="python:3-alpine"):
        """
        Initialise a pool of worker containers for the sandbox to execute code in
        :param image_suffix: A name to prefix to the image created by this process. If running multiple sandboxes, this
                             must be different between them all to avoid conflicts
        :param min_pool_size: The minimum number of unused containers
        :param min_available: The minimum number of containers that must be unused at a given time. If this is exceeded
                              more containers will automatically be spawned
        :param required_packages: List of required Python packages that must be installed in the worker containers.
                                  These should be written using pip's requirements.txt syntax.
        :param base_image: The base docker image to build from
        :return:
        """

        self.pool = Pool(self.client, image_suffix, min_pool_size, min_available, required_packages, base_image)
        self.pool.build_image()
        self.pool.start_pool_manager()

        import time

        while True:
            with self.pool.get_container() as container:
                print(container.exec_run("uname -a"))
            time.sleep(1)

        # import time
        # time.sleep(5)
        # print(self.pool._available_workers)
        # self.pool.stop_pool_manager()

        # self.pool._ensure_minimum_containers()
        #
        # while True:
        #     with self.pool.get_container() as container:
        #         print(container.exec_run("uname -a"))
        #
        #     self.pool._ensure_minimum_containers()
