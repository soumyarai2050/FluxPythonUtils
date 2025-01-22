from contextlib import contextmanager
import multiprocessing.shared_memory

# 3rd party imports
import pendulum


class LogAnalyzerSHM:
    MAX_SIZE = 35     # bytes

    def __init__(self, log_file_path: str, create: bool = False):
        self.log_file_path = log_file_path
        self.shm_lock_name = self.log_file_path+"_lock"
        if create:
            # Create new shared memory if it doesn't exist
            self.last_processed_timestamp_shm = multiprocessing.shared_memory.SharedMemory(
                name=self.log_file_path,
                create=True,
                size=self.MAX_SIZE
            )
            self.lock_shm = multiprocessing.shared_memory.SharedMemory(
                name=self.shm_lock_name,
                create=True,
                size=1
            )
        else:
            self.last_processed_timestamp_shm = multiprocessing.shared_memory.SharedMemory(name=self.log_file_path)
            self.lock_shm = multiprocessing.shared_memory.SharedMemory(name=self.shm_lock_name)

    def __del__(self):
        """Cleanup shared memory"""
        try:
            self.last_processed_timestamp_shm.close()
            self.lock_shm.close()
        except Exception:
            pass

    @contextmanager
    def _lock(self):
        """Implement simple spinlock using shared memory"""
        while True:
            # Attempt to acquire lock
            if self.lock_shm.buf[0] == 0:
                self.lock_shm.buf[0] = 1
                break
        try:
            yield
        finally:
            # Release lock
            self.lock_shm.buf[0] = 0

    def set(self, ts: str):
        """Save timestamp to shared memory"""
        with self._lock():
            ts_bytes = str(ts).encode('utf-8')
            if len(ts_bytes) > self.MAX_SIZE:
                raise ValueError("timestamp too large for shared memory buffer")
            # Clear the memory first
            self.last_processed_timestamp_shm.buf[:] = b'\x00' * self.MAX_SIZE
            # Write the new data
            self.last_processed_timestamp_shm.buf[:len(ts_bytes)] = ts_bytes

    def get(self):
        """Load timestamp from shared memory"""
        with self._lock():
            # Find the end of the JSON data (exclude null bytes)
            ts = bytes(self.last_processed_timestamp_shm.buf).rstrip(b'\x00')
            if ts:
                return pendulum.parse(ts.decode('utf-8'))
            return None
