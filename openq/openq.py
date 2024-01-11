import os
import sys
import json
import time
import redis
import logging
import datetime
import threading
from collections import deque
from json.decoder import JSONDecodeError
from uuid import uuid4
from openq.utils import CustomJSONEncoder, is_redis_running

DEBUG = True


class Message:
    """
    Represents a message object that can be processed by a consumer.

    Attributes:
        id (str): A unique identifier for the message.
        body (str): The content of the message.
        received_at (datetime or None): Timestamp when the message was received; None initially and set when consumed.
    """

    def __init__(self, id=None, body=None, received_at=None, timestamp=None):
        if id is None:
            self.id = str(uuid4())
        else:
            self.id = id
        self.body = body
        self.received_at = received_at
        if timestamp is None:
            self.timestamp = datetime.datetime.now()
        else:
            self.timestamp = timestamp

    def mark_received(self):
        """
        Marks the message as received by setting the current timestamp
        to the `received_at` attribute.
        """
        self.received_at = datetime.datetime.now()

    @classmethod
    def from_json(cls, json_str):
        """
        Creates a Message instance from a JSON string.

        Args:
            json_str (str): JSON string representing the message.

        Returns:
            Message: An instance of the message with the data from the JSON string.
        """
        data = json.loads(json_str)
        return cls(
            id=data.get("id"),
            body=data.get("body"),
            received_at=data.get("received_at"),
            timestamp=datetime.datetime.fromisoformat(data["timestamp"]),
        )


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
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

    def enqueue(self, message_body):
        """
        Simulate enqueuing messages into the queue adhering to FIFO order.
        """
        with self.lock:
            message = Message(body=message_body)
            # self.messages.append(message)
            self.redis_client.rpush(
                self.name, json.dumps(message.__dict__, cls=CustomJSONEncoder)
            )
        return message.id

    def dequeue(self):
        """
        Simulate dequeuing messages from the queue adhering to FIFO order,
        making them invisible for a defined timeout period.
        """
        with self.lock:
            message_data = self.redis_client.brpoplpush(
                self.name, f"{self.name}:processing", self.visibility_timeout
            )

            if not message_data:
                return None
            message_dict = json.loads(message_data)
            message = Message(**message_dict)
            # message = self.messages.popleft()
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
        with self.lock:
            result = self.redis_client.lrem(
                f"{self.name}",
                1,
                json.dumps(
                    {
                        "id": message_id,
                    },
                    cls=CustomJSONEncoder,
                ),
            )
            if result == 0:
                raise MessageNotFoundError(f"Message ID {message_id} not found")
            self._cancel_timer(message_id)

    @classmethod
    def _get_redis_client(cls, redis_client=None):
        """
        Get a Redis client instance.

        Args:
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Returns:
            redis.Redis: A Redis client instance.
        """
        return (
            redis_client
            if redis_client
            else redis.Redis(host="localhost", port=6379, db=0)
        )

    @classmethod
    def create_queue(cls, queue_name, visibility_timeout=300, redis_client=None):
        """
        Create a new queue.

        Args:
            queue_name (str): The name of the queue to create.
            visibility_timeout (int): Visibility timeout for the queue (default: 300 seconds).
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Raises:
            RuntimeError: If the queue already exists.
            Exception: If there is a problem with creating the queue.
        """
        try:
            redis_client = cls._get_redis_client(redis_client)
            if not redis_client.exists(queue_name):
                redis_client.lpush(
                    queue_name, ""
                )  # Create an empty list to signify the creation of the queue
            else:
                raise RuntimeError(f"Queue '{queue_name}' already exists.")
        except Exception as e:
            raise Exception(f"Failed to create queue '{queue_name}': {e}")

    @classmethod
    def delete_queue(cls, queue_name, redis_client=None):
        """
        Delete an existing queue.

        Args:
            queue_name (str): The name of the queue to delete.
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Raises:
            RuntimeError: If the queue does not exist.
            Exception: If there is a problem with deleting the queue.
        """
        try:
            redis_client = cls._get_redis_client(redis_client)
            if redis_client.exists(queue_name):
                redis_client.delete(queue_name)
            else:
                raise RuntimeError(f"Queue '{queue_name}' does not exist.")
        except Exception as e:
            raise Exception(f"Failed to delete queue '{queue_name}': {e}")

    @classmethod
    def list_queues(cls, redis_client=None):
        """
        List all the queues in the Redis instance.

        Args:
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Returns:
            List[str]: A list containing the names of all queues.
        """
        redis_client = cls._get_redis_client(redis_client)
        # Assuming queue names do not contain colon character (':')
        return [
            key.decode("utf-8") for key in redis_client.keys("*") if b":" not in key
        ]

    @classmethod
    def queue_size(cls, queue_name, redis_client=None):
        """
        Get the size of the queue.

        Args:
            queue_name (str): Name of the queue.
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Returns:
            int: Number of messages in the queue.
        """
        redis_client = cls._get_redis_client(redis_client)
        return redis_client.llen(queue_name)

    @classmethod
    def delete_all_queues(cls, pattern="*", redis_client=None):
        """
        Delete all queues matching a pattern.

        Args:
            pattern (str): Pattern to match queue names. Defaults to '*' which matches all queues.
            redis_client (Optional[redis.Redis]): An optional Redis client instance.

        Raises:
            Exception: If there is an error during the deletion process.
        """
        try:
            redis_client = cls._get_redis_client(redis_client)
            queues = redis_client.keys(pattern)

            if not queues:
                raise RuntimeError(f"No queues match the pattern '{pattern}'.")

            for queue_name in queues:
                redis_client.delete(queue_name)
        except Exception as e:
            raise Exception(
                f"Failed to delete queues matching pattern '{pattern}': {e}"
            )

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
            self.redis_client.lrem(
                f"{self.name}",
                1,
                json.dumps(message.__dict__, cls=CustomJSONEncoder),
            )
            # self.consumed_messages.remove(message)
            # Move message to dlq or re-queue as per your requirements
            if self.dlq:
                self.dlq.enqueue(message.body)
            else:
                self.redis_client.lpush(self.name, json.dumps(message.__dict__))

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

    def get_messages(self, queue):
        messages = self.redis_client.lrange(queue, 0, -1)
        decoded_messages = [Message.from_json(m.decode("utf-8")) for m in messages]
        return [m.__dict__ for m in decoded_messages]


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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not is_redis_running():
        logging.error(
            "🚨 Redis server is not running. Please start the Redis server and try again."
        )
        sys.exit(1)

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
