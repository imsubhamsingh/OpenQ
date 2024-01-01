import time
import datetime
import threading
from collections import deque
from uuid import uuid4


class Message:
    """
    Represents a message object that can be processed by a consumer.

    Attributes:
        id (str): A unique identifier for the message.
        body (str): The content of the message.
        received_at (datetime or None): Timestamp when the message was received; None initially and set when consumed.
    """

    def __init__(self, body):
        self.id = str(uuid4())
        self.body = body
        self.received_at = None

    def mark_received(self):
        """
        Marks the message as received by setting the current timestamp
        to the `received_at` attribute.
        """
        self.received_at = datetime.datetime.now()


class VisibilityTimeOutExpired(Exception):
    """Raised when the visibility timeout for a message has expired."""

    pass


class MessageNotFoundError(Exception):
    """Raised when the message ID is not found."""

    pass


class OpenQ:
    """
    A Simple Python in-memory message Queue Service

    Attributes:
        name (str): Name of the queue.
        messages (deque<Message>): A thread-safe double-ended queue that stores messages.
        visibility_timeout (int): The amount of time a message stays hidden after being dequeued before it becomes visible again (in seconds).
        dlq (OpenQ): Dead-letter queue where messages are moved after visibility timeout expires.
        lock (threading.Lock): A Lock object used to ensure thread-safe access to the queue.
    """

    def __init__(self, name, visibility_timeout=300, dlq=None):
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
            message.mark_received()
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
                            raise VisibilityTimeOutExpired(
                                "Visibility timeout expired; message re-queued."
                            )
                        self.messages.remove(message)  # Remove message from queue
                        print(f"Acknowledged Task ID: {message.id}")
                        return
        raise VisibilityTimeOutExpired("Message ID not found or already acknowledged.")

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
        Check if the visibility timeout has expired for the given message.

        Returns:
            True if the visibility timeout expired, False otherwise.
        """
        expiry_time = message.received_at + datetime.timedelta(
            seconds=self.visibility_timeout
        )
        return datetime.datetime.now() >= expiry_time


class Producer(threading.Thread):
    """
    The Producer class is a subclass of Thread that simulates the production
    of tasks. It puts task messages onto a provided queue and prints out a log
    message for each task produced with its corresponding queue name and task ID.
    Attributes:
        queue (Queue): an instance of a queue class which has an enqueue method
                    to add message to the queue.
        num_tasks (int): the number of tasks to produce and put in the queue.
    """

    def __init__(self, queue, num_tasks):
        super().__init__()
        self.queue = queue
        self.num_tasks = num_tasks

    def run(self):
        """
        Producing messages to queue
        """
        for i in range(self.num_tasks):
            message_body = f"Task {i}"
            message_id = self.queue.enqueue(message_body)
            print(f"📩 Produced to Queue: {self.queue.name} Task ID: {message_id}")
            time.sleep(1)


class Consumer(threading.Thread):
    """
    The Consumer class is a subclass of Thread that simulates the consumption
    of tasks. It retrieves task messages from the provided queue, processes them,
    and then deletes them from the queue.

    Attributes:
        queue (Queue): an instance of a queue class which has dequeue and delete
                    methods to remove and process messages from the queue.
    """

    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        """
        Consumes message and listens to queue
        """
        while True:
            message = self.queue.dequeue()
            if message:
                print(f"📨 Consumed Task ID: {message.id}, Body: {message.body}")
                try:
                    # Simulate processing time
                    time.sleep(2)
                    self.queue.delete(message.id)
                except VisibilityTimeOutExpired as e:
                    print(str(e))
            else:
                print(f"😴 Queue {self.queue.name} is empty, waiting for tasks...")
                time.sleep(1)


## Queue Creation Junction ##

# Create main queue and dead-letter queue
dlq = OpenQ(name="dead-letter-queue", visibility_timeout=10)
main_queue = OpenQ(name="main-queue", visibility_timeout=10, dlq=dlq)

# Set number of tasks to be produced
num_tasks = 5

# Using threading to simulate concurrent producing and consuming processes
producer_thread = Producer(main_queue, num_tasks)
consumer_thread = Consumer(main_queue)

producer_thread.start()
consumer_thread.start()


# The join() method is a synchronization mechanism that ensures that the main program waits
# for the threads to complete before moving on. Calling join() on producer_thread causes the
# main thread of execution to block until producer_thread finishes its task.
producer_thread.join()
consumer_thread.join()
