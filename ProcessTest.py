import multiprocessing
import time
from DataBase import SynchronizedDataBase


def reader(file_name, reader_id):
    """
    Reader function to read values from the database.
    Each reader process will attempt to read values for keys 0 to 4.
    """
    db = SynchronizedDataBase(file_name, "processes")
    print(f"Reader {reader_id} started")
    for i in range(5):
        value = db.value_get(i)
        print(f"Reader {reader_id} got {value} for key {i}")
        time.sleep(1)
    print(f"Reader {reader_id} finished")


def writer(file_name, writer_id):
    """
    Writer function to write values to the database.
    Each writer process will attempt to write its id as the value for keys 0 to 9.
    If a key already has a value, it will be skipped.
    """
    db = SynchronizedDataBase(file_name, "processes")
    print(f"Writer {writer_id} started")
    for i in range(10):
        success = db.value_set(i, writer_id)
        if success:
            print(f"Writer {writer_id} set value {writer_id} for key {i}")
        else:
            print(f"Writer {writer_id} skipped key {i} because it already exists")
        time.sleep(1)
    print(f"Writer {writer_id} finished")


def test_processes():
    """
    Function to test the SynchronizedDataBase with multiple reader and writer processes.
    """
    file_name = 'test.pkl'

    writer_processes = []
    for i in range(2):
        p = multiprocessing.Process(target=writer, args=(file_name, i))
        writer_processes.append(p)
        p.start()

    reader_processes = []
    for i in range(10):
        p = multiprocessing.Process(target=reader, args=(file_name, i))
        reader_processes.append(p)
        p.start()

    for p in writer_processes + reader_processes:
        p.join()


if __name__ == "__main__":
    print("Testing with processes")
    test_processes()
