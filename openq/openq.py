import time
import threading
from collections import deque
from uuid import uuid4


class Message:
    def __init__(self, body):
        self.id = str(uuid4())
        self.body = body
        self.received_at = None


class VisiblityTimeOutExpired(Exception):
    pass


class OpenQ:
    """
    A Simple Python in-memory message Queue Service
    """

    def __init__(self, name, visibility_timeout=30, dlq=None):
        self.name = name
        self.messages = deque()
        self.visibility_timeout = visibility_timeout
        self.dlq = dlq  # Dead-letter queue
        self.lock = threading.Lock()

    def enqueue(self, message_body):
        """
        Simulate enqueuing messages into the queue
        """
        with self.lock:
            message = Message(message_body)
            self.messages.appendleft(message)
            return message.id

    def dequeue(self):
        """
        Simulate dequeuing messages from the queue, making them invisible for a defined timeout
        Also starts a timer on each message that will call _requeue after the visibility timeout expires.
        """
        with self.lock:
            if not self.messages:
                return None
            message = self.messages.pop()
            message.received_at = int(time.time())
            t = threading.Timer(self.visibility_timeout, self._requeue, [message])
            t.start()
            return message

    def delete(self, message_id):
        """
        Simulate consumers sending back acknowledgment of message processing
        """
        with self.lock:
            if any(m.id == message_id for m in self.messages):
                for message in self.messages:
                    if message.id == message_id:
                        if self._visibility_timeout_expired(message):
                            raise VisiblityTimeOutExpired(
                                "Visibility timeout expired; message requeued."
                            )
                        self.messages.remove(message)  # Remove message from queue
                        print(f"Acknowledged Task ID: {message.id}")
                        return
        raise VisiblityTimeOutExpired("Message ID not found or already acknowledged.")

    def _requeue(self, message):
        """
        Helper method to renqueue messages of they timeout
        """
        with self.lock:
            if self._visibility_timeout_expired(message):
                if self.dlq:
                    print(f"Task ID: {message.id} moved to DLQ")
                    # Move to DLQ
                    self.dlq.enqueue(message.body)
                else:
                    # Requeue in the current queue
                    self.messages.appendleft(message)

    def _visibility_timeout_expired(self, message):
        """
        Helper method to check visibility timeout
        """
        now = int(time.time())
        expiry_time = message.received_at + self.visibility_timeout
        return now >= expiry_time


def producer(queue, num_tasks):
    """
    Producing messages to queue
    """
    for i in range(num_tasks):
        message_body = f"Task {i}"
        message_id = queue.enqueue(message_body)
        print(f"Produced to Queue: {queue.name} Task ID: {message_id}")
        time.sleep(1)


def consumer(queue):
    """
    Consumes message and listen to queue
    """
    print("### CONSUMER ###")
    while True:
        message = queue.dequeue()
        if message:
            print(f"Consumed Task ID: {message.id}, Body: {message.body}")
            try:
                # Simulate processing time
                time.sleep(2)
                queue.delete(message.id)
            except VisiblityTimeOutExpired as e:
                print(str(e))
        else:
            print(f"{queue.name} is empty, waiting for tasks...")
            time.sleep(1)


## Queue Creation Junction ##

# Create main queue and dead-letter queue
dlq = OpenQ(name="dead-letter-queue", visibility_timeout=10)
main_queue = OpenQ(name="main-queue", visibility_timeout=10, dlq=dlq)

# Set number of tasks to be produced
num_tasks = 5

# Using threading to simulate concurrent producing and consuming processes
producer_thread = threading.Thread(target=producer, args=(main_queue, num_tasks))
consumer_thread = threading.Thread(target=consumer, args=(main_queue,))

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()
