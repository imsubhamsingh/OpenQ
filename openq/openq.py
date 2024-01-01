from queue import Queue
import threading
import time

# The shared queue
message_queue = Queue()

def producer():
    # Simulating message production
    for i in range(5):
        message = f"Message {i}"
        print(f"Producing {message}")
        message_queue.put(message)
        time.sleep(1)

def consumer():
    while True:
        # Waits for the next message
        message = message_queue.get()
        print(f"Consuming {message}")
        
        # Simulate acknowledging the message processing is complete
        message_queue.task_done()
        time.sleep(2)

# Starting a producer and consumer thread
producer_thread = threading.Thread(target=producer)
consumer_thread = threading.Thread(target=consumer, daemon=True)

producer_thread.start()
consumer_thread.start()

producer_thread.join()  # Wait for the producer to finish
message_queue.join()   # Wait for all messages to get processed
