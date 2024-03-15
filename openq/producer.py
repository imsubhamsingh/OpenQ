import time
import logging
import threading
from openq import OpenQ

DEBUG = True


class OpenQProducer(threading.Thread):
    """
    The Producer class is responsible for producing tasks and adding them
    to the provided queue. Each task produced is logged with the queue name
    and a unique task ID.

    Attributes:
        queue (Queue): An instance of a queue class which supports an enqueue method.
        num_tasks (int): Number of tasks to produce.
        sleep_time (float): Seconds to sleep after producing each task.
    """

    def __init__(self, queue, num_tasks, sleep_time=1.0, max_retries=3):
        """
        Initialize the Producer instance with a queue, number of tasks, and sleep interval.

        Args:
            queue (Queue): The queue instance where tasks will be added.
            num_tasks (int): The total number of tasks to be produced.
            sleep_time (float): Time in seconds to wait between task productions.
            max_retries (int): Max number of retries on messaging falure.
        """
        super().__init__(
            daemon=True
        )  # Make thread a daemon so it won't prevent program termination
        self.queue = queue
        self.num_tasks = max(0, num_tasks)
        self.sleep_time = max(0, sleep_time)
        self.max_retries = max_retries

    def run(self):
        """
        Overwrites the Thread.run() method. Produces num_tasks tasks, each followed by a sleep period.
        """
        for i in range(self.num_tasks):
            message_body = f"Hello Task {i}"
            retry_count = 0
            while retry_count <= self.max_retries:
                message_id = self._produce_message(message_body)
                if message_id:
                    logging.info(
                        f"📩 Produced Task ID: {message_id} to Queue: {self.queue.name}"
                    )
                    break
                else:
                    retry_count += 1
                    logging.warning(
                        f"Retry {retry_count}/{self.max_retries} for producing Task {i}"
                    )
                    time.sleep(
                        min(2**retry_count, 30)
                    )  # Exponential backoff capped at 30 seconds

            if retry_count > self.max_retries:
                logging.error(
                    f"😩 Failed to produce Task {i} to Queue '{self.queue.name}'"
                )

            # Sleep only if there are more tasks to produce to avoid unnecessary delay after the last task.
            if i < self.num_tasks - 1:
                time.sleep(self.sleep_time)

    def _produce_message(self, message_body):
        try:
            return self.queue.enqueue(message_body)
        except Exception as e:
            logging.exception(f"📮 An error occurred while enqueueing: {e}")
            return None


def run_producer():
    # Create main queue and dead-letter queue
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="main-queue", visibility_timeout=100, dlq=dlq)

    # Set number of tasks to be produced
    num_tasks = 5

    # Create a producer instance
    producer_thread = OpenQProducer(main_queue, num_tasks)

    # Start producing messages
    producer_thread.start()

    # Wait for all messages to be produced
    producer_thread.join()


if __name__ == "__main__":
    run_producer()
