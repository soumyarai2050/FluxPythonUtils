from _ctypes import Structure, POINTER, pointer
from ctypes import c_char, c_long, CDLL, c_int
from typing import ClassVar


class pthread_mutex_t(Structure): # NOQA, class name not caps to align with underlying type
    # size had-coded as found in CPP, assert in CPP if pthread_mutex_t size is not 40
    _fields_ = [
        ("__data", c_char * 40)
    ]


class timespec(Structure): # NOQA, class name not caps to align with underlying type
    # size had-coded as found in CPP, assert in CPP if pthread_mutex_t size is not 40
    _fields_ = [
        ("tv_sec", c_long),
        ("tv_nsec", c_long)
    ]


class PThreadShmMutex:
    libc: ClassVar = CDLL(None)

    pthread_mutex_lock: ClassVar = libc.pthread_mutex_lock
    pthread_mutex_lock.argtypes = [POINTER(pthread_mutex_t)]
    pthread_mutex_lock.restype = c_int

    pthread_mutex_trylock: ClassVar = libc.pthread_mutex_trylock
    pthread_mutex_trylock.argtypes = [POINTER(pthread_mutex_t)]
    pthread_mutex_trylock.restype = c_int

    pthread_mutex_timedlock: ClassVar = libc.pthread_mutex_timedlock
    pthread_mutex_timedlock.argtypes = [POINTER(pthread_mutex_t), POINTER(timespec)]
    pthread_mutex_timedlock.restype = c_int

    pthread_mutex_unlock: ClassVar = libc.pthread_mutex_unlock
    pthread_mutex_unlock.argtypes = [POINTER(pthread_mutex_t)]
    pthread_mutex_unlock.restype = c_int

    # gettime: needed for pthread_mutex_timedlock
    clock_gettime: ClassVar = libc.clock_gettime
    clock_gettime.argtypes = [c_int, POINTER(timespec)]
    clock_gettime.restype = c_int

    def __init__(self, pthread_mutex: pthread_mutex_t, timedlock_timeout_sec: int = int(20)):
        self.pthread_mutex = pointer(pthread_mutex)
        self.pthread_mutex_ptr = pthread_mutex
        self.timeout_sec = timedlock_timeout_sec

    def lock(self) -> c_int:
        return self.pthread_mutex_lock(self.pthread_mutex_ptr)

    def unlock(self) -> c_int:
        return self.pthread_mutex_unlock(self.pthread_mutex_ptr)

    def trylock(self) -> c_int:
        """ returns 0 if lock successful"""
        return self.pthread_mutex_trylock(self.pthread_mutex_ptr)

    def timedlock(self) -> int:
        """ returns 0 if lock successful"""
        cur_time = timespec()  # create empty object populated by clock_gettime below with current time
        self.clock_gettime(0, pointer(cur_time))
        timeout = timespec(tv_sec=cur_time.tv_sec + self.timeout_sec, tv_nsec=0)
        return self.pthread_mutex_timedlock(self.pthread_mutex_ptr, pointer(timeout))

    def try_timedlock(self) -> int:
        """ returns 0 if lock successful"""
        if self.trylock() == 0:
            return 0
        else:
            return self.timedlock()
