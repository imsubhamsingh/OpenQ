import os
import json
import time
import logging
import datetime
import threading
from collections import deque
from json.decoder import JSONDecodeError
from uuid import uuid4

# Configure the logging system
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
DEBUG = True


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
        self.timestamp = datetime.datetime.now()

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
    pass


class OpenQ:
    """
    A Simple Python in-memory FIFO message Queue Service

    Attributes:
        name (str): Name of the queue.
        messages (deque<Message>): A thread-safe double-ended queue that stores messages.
        consumed_message (deque<Message>): A thread-safe double-ended queue that stores consumed messages.
        visibility_timeout (int): The amount of time a message stays hidden after being dequeued before it becomes visible again (in seconds).
        dlq (OpenQ): Dead-letter queue where messages are moved after visibility timeout expires.
        lock (threading.Lock): A Lock object used to ensure thread-safe access to the queue.
        message_timers (dict): A dict contains list of message.id as key and timer object as value.
    """

    def __init__(self, name, visibility_timeout=300, dlq=None):
        self.name = name
        self.messages = deque()  # store processed messages
        self.consumed_messages = deque()  # Store consumed messages
        self.visibility_timeout = visibility_timeout
        self.dlq = dlq  # Dead-letter queue
        self.lock = threading.Lock()
        self.message_timers = {}

    def enqueue(self, message_body):
        """
        Simulate enqueuing messages into the queue adhering to FIFO order.
        """
        with self.lock:
            message = Message(message_body)
            self.messages.append(message)
        return message.id

    def dequeue(self):
        """
        Simulate dequeuing messages from the queue adhering to FIFO order,
        making them invisible for a defined timeout period.
        """
        with self.lock:
            if not self.messages:
                return None
            message = self.messages.popleft()
            message.mark_received()

            # Add to consumed messages upon dequeue
            self.consumed_messages.append(message)

            # Start a timer for visibility timeout
            timer = threading.Timer(
                self.visibility_timeout, self._handle_visibility_timeout, [message]
            )
            # Track the timer by message ID
            self.message_timers[message.id] = timer
            timer.start()

            return message

    def delete(self, message_id):
        """
        Simulate consumers sending back acknowledgment of message processing
        """
        with self.lock:
            # Remove the message from consumed_messages based on message_id
            for msg in list(
                self.consumed_messages
            ):  # Iterate over a copy to avoid mutation during iteration
                if msg.id == message_id:
                    if DEBUG:
                        print(f"msg.id {msg.id} ==== message_id {message_id}")
                    logging.info(
                        f"Deleting message with ID {message_id} from consumed messages."
                    )
                    self.consumed_messages.remove(msg)
                    # Cancel and remove the timer associated with the message
                    self._cancel_timer(message_id)
                    logging.debug(
                        f"Message with ID {message_id} has been acknowledged and deleted."
                    )
                    return

            # If we reach this point, the message was not found in consumed_messages
            logging.warning(
                f"Attempted to delete non-existent or already processed message with ID {message_id}."
            )
            raise MessageNotFoundError(f"Message ID {message_id} not found")

    def _requeue_or_move_to_dlq(self, message):
        """
        Helper method to renqueue messages of they timeout
        """
        with self.lock:
            timer = self.message_timers.pop(message.id, None)
            # If timer is None, it means that the delete operation was successful
            if timer is not None:
                try:
                    pass
                    # self.consumed_messages.remove(message)
                except ValueError:
                    pass  # Message is already removed

                message.received_at = None
                if self.dlq:
                    print(f"Task ID: {message.id} moved to DLQ")
                    # Move to DLQ
                    self.dlq.enqueue(message.body)
                else:
                    # TODO: Here correctness depends on the intended behavior.
                    # RN our intention is for timed-out messages to retain their original position
                    # in the queue upon requeuing (strict FIFO), so reinserting at the front.
                    # Will see later if we have to penalize messages that time-out by placing them
                    # at the end of the queue (effectively deprioritizing them) in that case we will
                    # use .append() method
                    self.messages.appendleft(message)

    def _handle_visibility_timeout(self, message):
        with self.lock:
            if message in self.consumed_messages:
                # self.consumed_messages.remove(message)
                # Move message to dlq or re-queue as per your requirements
                if self.dlq:
                    self.dlq.enqueue(message)
                else:
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

    def _save_to_disk(self, message_queue, filepath):
        """
        Save the current state to JSON files for persistence.
        """
        try:
            with open(filepath, "w") as f:
                json.dump(self._serialize_messages(message_queue), f)
        except IOError as e:
            logging.error(f"Error saving tasks to {filepath}: {e}")

    def _serialize_messages(self, message_queue):
        """
        Helper method to serialize messages for saving to disk
        """
        return [
            {
                "id": msg.id,
                "body": msg.body,
                "received_at": msg.received_at.isoformat() if msg.received_at else None,
            }
            for msg in message_queue
        ]

    def _load_from_disk(self, filepath):
        """
        Load messages from a JSON file into the message queue during initialization.
        """
        if os.path.exists(filepath):
            try:
                with open(filepath, "r") as f:
                    loaded_messages = json.load(f)

            except (FileNotFoundError, JSONDecodeError) as e:
                # Catch any exception during file read and JSON decoding
                # If JSON contents are invalid, initialize with an empty list
                logging.error(
                    f"Failed to load storage file {filepath} ({e}); initializing an empty queue."
                )
                return deque()
            return deque(
                [self._deserialize_message(msg_data) for msg_data in loaded_messages]
            )
        return deque()

    def _deserialize_message(self, msg_data):
        received_at = (
            datetime.datetime.fromisoformat(msg_data["received_at"])
            if msg_data["received_at"]
            else None
        )
        msg = Message(msg_data["body"])
        msg.id = msg_data["id"]
        msg.received_at = received_at
        return msg

    def _cancel_timer(self, message_id):
        timer = self.message_timers.get(message_id)
        # Only attempt to cancel the timer if it exists
        if timer:
            timer.cancel()
            del self.message_timers[message_id]
            logging.debug(
                f"Visibility timer canceled for message with ID {message_id}."
            )

    def purge(self):
        """
        Purge all messages from the queue and cancel all running timers along with storage file
        """
        with self.lock:
            self.messages.clear()
            # Cancel all timers related to this queue's messages
            for timer in self.message_timers.values():
                timer.cancel()
            self.message_timers.clear()

            # Delete the storage file if exists
            if os.path.exists(self.storage_file):
                os.remove(self.storage_file)

            print(f"The queue '{self.name}' and storage has been purged")


class Producer(threading.Thread):
    """
    The Producer class is responsible for producing tasks and adding them
    to the provided queue. Each task produced is logged with the queue name
    and a unique task ID.

    Attributes:
        queue (Queue): An instance of a queue class which supports an enqueue method.
        num_tasks (int): Number of tasks to produce.
        sleep_time (float): Seconds to sleep after producing each task.
    """

    def __init__(self, queue, num_tasks, sleep_time=1.0):
        """
        Initialize the Producer instance with a queue, number of tasks, and sleep interval.

        Args:
            queue (Queue): The queue instance where tasks will be added.
            num_tasks (int): The total number of tasks to be produced.
            sleep_time (float): Time in seconds to wait between task productions.
        """
        super().__init__(
            daemon=True
        )  # Make thread a daemon so it won't prevent program termination
        self.queue = queue
        self.num_tasks = max(0, num_tasks)
        self.sleep_time = max(0, sleep_time)

    def run(self):
        """
        Overwrites the Thread.run() method. Produces num_tasks tasks, each followed by a sleep period.
        """
        for i in range(self.num_tasks):
            message_body = f"Hello Task {i}"
            message_id = self._produce_message(message_body)
            if message_id:
                logging.info(
                    f"📩 Produced to Queue: '{self.queue.name}' - Task ID: {message_id}"
                )
            else:
                logging.info(f"Failed to produce Task {i} to Queue '{self.queue.name}'")

            # Sleep only if there are more tasks to produce to avoid unnecessary delay after the last task.
            should_sleep = i < self.num_tasks - 1
            self._sleep_if_necessary(should_sleep)

    def _produce_message(self, message_body):
        try:
            return self.queue.enqueue(message_body)
        except Exception as e:
            logging.exception(f"📮 An error occurred while enqueueing: {e}")
            return None

    def _sleep_if_necessary(self, should_sleep):
        if should_sleep:
            time.sleep(self.sleep_time)


class Consumer(threading.Thread):
    """
    The Consumer class is designed to consume tasks from a queue, process,
    and delete those tasks.

    Attributes:
        queue (Queue): An instance of a queue class which has methods dequeue and
                       delete for message manipulation.
        processing_time (float): Time taken to process a task.
        empty_queue_sleep (float): Time to sleep when the queue is empty.
    """

    def __init__(self, queue, processing_time=2.0, empty_queue_sleep=1.0):
        """
        Constructor to initialize the Consumer thread.

        Args:
            queue (Queue): The queue from which messages will be consumed.
            processing_time (float): Time to simulate task processing.
            empty_queue_sleep (float): Sleep duration when the queue is found empty.
        """
        super().__init__()
        self.queue = queue
        self.processing_time = max(0.0, processing_time)
        self.empty_queue_sleep = max(0.0, empty_queue_sleep)
        self._running = True

    def stop(self):
        """
        Signals the consumer to gracefully shutdown.
        """
        self._running = False

    def run(self):
        """
        The main execution method of the thread that continues to consume and process tasks.
        """
        while self._running:
            message = None
            try:
                message = self.queue.dequeue()
                if message:
                    self._process_message(message)
                    if DEBUG:
                        print("----" * 50)
                        print(
                            f"MESSAGES: {[msg.id for msg in list(self.queue.messages)]}"
                        )
                        print(
                            f"CONSUMED_MESSAGES: {[msg.id for msg in list(self.queue.consumed_messages)]}"
                        )
                        print(f"MAIN_QUEUE_SIZE: {len(self.queue.messages)})")
                        print(
                            f"CONSUMED_QUEUE_SIZE: {len(self.queue.consumed_messages)}"
                        )
                        print("----" * 50)
                else:
                    self._wait_for_tasks()
            except Exception as error:
                logging.exception(f"An error occurred during message handling: {error}")
                if message:
                    self._handle_failed_dequeue(message)

    def _process_message(self, message):
        """
        Process a message and deletes it from the queue afterwards.
        """
        try:
            # Message processing logic here
            logging.info(f"📨 Consuming Task ID: {message.id}")
            # Simulate message processing with a sleep
            time.sleep(self.processing_time)
            # Delete the message from the queue after processing
            # self.queue.delete(message.id)
        except Exception as e:
            logging.exception(f"Failed to process or delete task: {str(e)}")

    def _wait_for_tasks(self):
        """
        If the queue is empty, logs the condition and sleeps for specified duration.
        """
        logging.info(
            f"😴 Queue '{self.queue.name}' is empty. Consumer is waiting for tasks."
        )
        time.sleep(self.empty_queue_sleep)

    def _handle_failed_dequeue(self, message):
        """
        Handles the case where a message was dequeued but not processed successfully.
        """
        logging.warning(
            f"Encountered an issue with Task ID: {message.id}. It will be reprocessed."
        )


## Queue Creation Junction ##

# Create main queue and dead-letter queue
dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
main_queue = OpenQ(name="main-queue", visibility_timeout=100, dlq=dlq)

# Set number of tasks to be produced
num_tasks = 5

# Using threading to simulate concurrent producing and consuming processes
producer_thread = Producer(main_queue, num_tasks)
consumer_thread = Consumer(main_queue)

try:
    producer_thread.start()
    consumer_thread.start()

except KeyboardInterrupt:
    logging.info("Terminating the threads...")
    # The join() method is a synchronization mechanism that ensures that the main program waits
    # for the threads to complete before moving on. Calling join() on producer_thread causes the
    # main thread of execution to block until producer_thread finishes its task.
    consumer_thread.stop()
    consumer_thread.join()
    producer_thread.join()
