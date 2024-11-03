"""
author: Alexey Aleshchenok
date: 2023-01-11
"""

import pickle
import threading
import multiprocessing


class DataBase:
    """
    A simple in-memory key-value database.
    """
    def __init__(self):
        self.data = {}

    def set_value(self, key, value):
        """
        Sets the value for a given key. Returns False if the key already exists.
        """
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def get_value(self, key):
        """
        Gets the value associated with the given key. Returns None if the key does not exist.
        """
        if key in self.data:
            return self.data.get(key, None)

    def delete(self, key):
        """
        Deletes the value associated with the given key. Returns the deleted value, or None if the key does not exist.
        """
        if key in self.data:
            return self.data.pop(key, None)


class SerializedDataBase(DataBase):
    """
    A database that supports serialization to a file.
    """
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.load()

    def save(self):
        """
        Saves the current state of the database to a file using pickle.
        """
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)

    def load(self):
        """
        Loads the state of the database from a file using pickle.
        """
        try:
            with open(self.filename, 'rb') as f:
                self.data = pickle.load(f)
        except (FileNotFoundError, EOFError):
            self.data = {}


class SynchronizedDataBase(SerializedDataBase):
    """
    A thread-safe or process-safe database with read/write locks and limited concurrent reads.
    """
    def __init__(self, filename, mode):
        super().__init__(filename)
        self.data_lock = multiprocessing.Lock()
        self.read_limit = 10

        # Initialize synchronization primitives based on the mode
        if mode == 'threads':
            self.read_lock = threading.Lock()
            self.write_lock = threading.Lock()
            self.read_semaphore = threading.Semaphore(self.read_limit)
        elif mode == 'processes':
            self.read_lock = multiprocessing.Lock()
            self.write_lock = multiprocessing.Lock()
            self.read_semaphore = multiprocessing.Semaphore(self.read_limit)

    def value_set(self, key, value):
        """
        Sets the value for a given key with write-lock and read-lock protection.
        """
        with self.write_lock:
            with self.read_lock:
                for _ in range(self.read_limit):
                    self.read_semaphore.acquire()
                with self.data_lock:
                    result = super().set_value(key, value)
                    self.save()
                for _ in range(self.read_limit):
                    self.read_semaphore.release()
        return result

    def value_get(self, key):
        """
        Gets the value for a given key with read-lock protection.
        """
        if self.read_semaphore.acquire():
            self.load()
            data = super().get_value(key)
            self.read_semaphore.release()
            return data

    def value_delete(self, key):
        """
        Deletes the value for a given key with write-lock and read-lock protection.
        """
        with self.write_lock:
            with self.read_lock:
                for _ in range(self.read_limit):
                    self.read_semaphore.acquire()
                with self.data_lock:
                    result = super().delete(key)
                    self.save()
                for _ in range(self.read_limit):
                    self.read_semaphore.release()
        return result


if __name__ == "__main__":
    key_ = 0
    value_ = 1
    file_name = 'test.pkl'

    db1 = DataBase()
    assert db1.set_value(key_, value_) is True
    assert db1.get_value(key_) == value_
    db1.delete(key_)
    assert db1.get_value(key_) != value_

    db2 = SynchronizedDataBase(file_name, 'threads')
    assert db2.set_value(key_, value_) is True
    assert db2.get_value(key_) == value_
    db2.delete(key_)
    assert db2.get_value(key_) != value_

    db3 = SynchronizedDataBase(file_name, 'processes')
    assert db3.set_value(key_, value_) is True
    assert db3.get_value(key_) == value_
    db3.delete(key_)
    assert db3.get_value(key_) != value_
