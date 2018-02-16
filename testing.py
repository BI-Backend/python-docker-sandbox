from python_docker_sandbox.sandbox import Sandbox
import logging

logging.basicConfig(level=logging.INFO)


def main():
    # sandbox = Sandbox(base_url="tcp://192.168.99.100:2376",
    #                   client_cert="/Users/camerong/.docker/machine/certs/cert.pem",
    #                   client_key="/Users/camerong/.docker/machine/certs/key.pem",
    #                   client_verify="/Users/camerong/.docker/machine/certs/ca.pem")
    sandbox = Sandbox()
    sandbox.init_pool("testing", required_packages=["tabulate", "flask"], min_pool_size=10)


if __name__ == "__main__":
    main()
