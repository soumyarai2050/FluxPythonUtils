from pathlib import PurePath
from datetime import datetime
import time

from FluxPythonUtils.scripts.general_utility_functions import configure_logger, get_cpu_usage, get_ram_memory_usage, \
    get_disk_usage


if __name__ == "__main__":
    def main():
        log_dir: PurePath = PurePath(__file__).parent.parent / "log"
        datetime_str: str = datetime.now().strftime("%Y%m%d")
        configure_logger("debug", str(log_dir), f"server_monitoring_{datetime_str}.log")

        for i in range(3600):
            get_cpu_usage()
            get_ram_memory_usage()
            get_disk_usage()
            time.sleep(5)

    main()
