import threading
import time
from DataBase import SynchronizedDataBase


def reader(db, reader_id):
    """
    Reader function to read values from the database.
    Each reader will attempt to read values for keys 0 to 4.
    """
    print(f"Reader {reader_id} started")
    for i in range(5):
        value = db.value_get(i)
        print(f"Reader {reader_id} got {value} for key {i}")
        time.sleep(1)
    print(f"Reader {reader_id} finished")


def writer(db, writer_id):
    """
    Writer function to write values to the database.
    Each writer will attempt to write its id as the value for keys 0 to 9.
    If a key already has a value, it will be skipped.
    """
    print(f"Writer {writer_id} started")
    for i in range(10):
        success = db.value_set(i, writer_id)
        if success:
            print(f"Writer {writer_id} set value {writer_id} for key {i}")
        else:
            print(f"Writer {writer_id} skipped key {i} because it already exists")
        time.sleep(1)
    print(f"Writer {writer_id} finished")


def thread_test():
    """
    Function to test the SynchronizedDataBase with multiple reader and writer threads.
    """
    db = SynchronizedDataBase('test.pkl', 'threads')

    writer_threads = []
    for i in range(2):
        t = threading.Thread(target=writer, args=(db, i))
        writer_threads.append(t)
        t.start()

    reader_threads = []
    for i in range(10):
        t = threading.Thread(target=reader, args=(db, i))
        reader_threads.append(t)
        t.start()

    for t in writer_threads + reader_threads:
        t.join()


if __name__ == "__main__":
    print("Testing with threads")
    thread_test()
