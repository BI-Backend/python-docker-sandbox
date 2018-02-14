#!/usr/bin/env python3
"""
This script is executed inside containers managed by the sandbox.  It will cause the sandbox to exit after a
predetermined period of time, this timeout can be reset by the pool manager by calling a command within the container.
This functionality prevents containers being left running if the sandbox application crashes or exits without cleanly
shutting down containers.  The script is copied into the container image on creation.

Commands:
    container_timeout.py timeout - Script will run until timeout has elapsed without being reset, will then exit
    container_timeout.py reset - Will reset the timeout period
"""
import time
import sys

TIMEOUT_SECONDS = 30
CHECK_INTERVAL_SECONDS = 1
TIMESTAMP_FILE = "/var/run/container_timeout.ts"


def do_timeout():
    reset_timeout()
    while True:
        with open(TIMESTAMP_FILE, "r") as f:
            file_timestamp = int(f.read())
            current_time = time.time()
            if current_time >= file_timestamp:
                sys.exit()
        time.sleep(1)


def reset_timeout():
    current_time = int(time.time())
    next_timeout = current_time + TIMEOUT_SECONDS
    with open(TIMESTAMP_FILE, "w") as f:
        f.write(str(next_timeout))


def main():
    if len(sys.argv) < 2:
        print("Must provide a command, either \"timeout\" or \"reset\"")
    elif sys.argv[1] == "timeout":
        do_timeout()
    elif sys.argv[1] == "reset":
        reset_timeout()
    else:
        print("Command not understood, must be either \"timeout\" or \"reset\"")

if __name__ == "__main__":
    main()