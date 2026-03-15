import time 
import concurrent.futures

start = time.perf_counter()

def do_something_with_threading(seconds):
    print(f"Sleeping {seconds} second...")
    time.sleep(seconds)
    return f'Done Sleeping ... the thread {seconds}'

def do_something(seconds=1):
    print(f"WITHOUT THREADING")
    time.sleep(seconds)
    return f"Done Sleeping ..."

with concurrent.futures.ThreadPoolExecutor() as executor:
    secs = [1, 1, 1, 1, 1]
    result = [executor.submit(do_something_with_threading, sec) for sec in secs]
    # as completed
    for f in concurrent.futures.as_completed(result):
        print(f.result())

end = time.perf_counter()

print(f"Finished in {end - start:.2f} seconds")

start2 = time.perf_counter()
print()
print()
print()
print()
print("WITHOUT THREADING:\n")
do_something()
do_something()
do_something()
do_something()
do_something()
end2 = time.perf_counter()

print(f"Finished at: {start2 - end2} seconds")