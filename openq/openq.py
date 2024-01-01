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

    def __init__(self, visiblity_timeout=30):
        self.messages = deque()
        self.visiblity_timeout = visiblity_timeout
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
        """
        with self.lock:
            if not self.messages:
                return None
            message = self.messages.pop()
            message.received_at = int(time.time())
            return message

    def delete(self, message_id):
        """
        Simulate consumers sending back acknowledgment of message processing
        """
        with self.lock:
            # In a real scenario, you'd search by ID and remove the specific message
            # For simplicity, we're assuming the consumer processes the last received message
            if not self.messages or self.messages[-1].id != message_id:
                raise VisiblityTimeOutExpired(
                    "Message ID not found or already acknowledged."
                )

            if self._visibility_timeout_expired(self.messages[-1]):
                raise VisiblityTimeOutExpired(
                    "Visibility timeout expired; message requeued."
                )

            # Delete the message (acknowledgement of successful processing)
            self.messages.pop()

    def _visibility_timeout_expired(self, message):
        """
        Helper method to check visibility timeout
        """
        now = int(time.time())
        expiry_time = message.received_at + message.visiblity_timeout
        return now >= expiry_time


def producer(queue):
    """
    Producing messages to queue
    """
    for i in range(5):
        message_body = f"Task {i}"
        message_id = queue.enqueue(message_body)
        print(f"Produced Task ID: {message_id}")
        time.sleep(1)


def consumer(queue):
    """
    Consumes message and listen to queue
    """
    while True:
        message = queue.dequeue()
        if message:
            print(f"Consumed Task ID: {message.id}, Body: {message.body}")
            try:
                # Simulate processing time
                time.sleep(2)
                queue.delete(message.id)
                print(f"Acknowledged Task ID: {message.id}")
            except VisiblityTimeOutExpired as e:
                print(str(e))
        else:
            print("Queue is empty, waiting for tasks...")
            time.sleep(1)


# Create an instance of the queue with a short visibility timeout for demonstration

openq = OpenQ(visiblity_timeout=10)

producer_thread = threading.Thread(target=producer, args=(openq,))
consumer_thread = threading.Thread(target=consumer, args=(openq,))

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()
