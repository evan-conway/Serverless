import time

start_time = time.time()
while time.time() - start_time < 20:
    print("Loop")
    time.sleep(1)