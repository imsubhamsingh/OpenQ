import sys
import logging

sys.path.append("/Users/hackerearth386/projects/OpenQ")
from openq import OpenQ, Consumer, Producer
from openq.utils import is_redis_running


class Task1Consumer(Consumer):
    def __init__(self, queue, processing_time=2.0, empty_queue_sleep=1.0):
        super().__init__(queue, processing_time, empty_queue_sleep)

    def _process_message(self, message):
        # Specialized processing logic for Task1
        logging.info(f"Task1Consumer processing Task ID: {message.id}")
        # ... Custom processing steps for Task1 ...
        # Finally, acknowledge the message after processing
        self.queue.acknowledge(message.id)
        logging.info(f"Task1 processed and acknowledged Task ID: {message.id}")


class Task2Consumer(Consumer):
    def __init__(self, queue, processing_time=3.0, empty_queue_sleep=1.5):
        super().__init__(queue, processing_time, empty_queue_sleep)

    def _process_message(self, message):
        # Specialized processing logic for Task2
        logging.info(f"Task2Consumer processing Task ID: {message.id}")
        # ... Custom processing steps for Task2 ...
        # Finally, acknowledge the message after processing
        self.queue.acknowledge(message.id)
        logging.info(f"Task2 processed and acknowledged Task ID: {message.id}")


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

    # Create main queue, queue for task1, queue for task2, and dead-letter queue
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="demo", visibility_timeout=100, dlq=dlq)
    task1_queue = OpenQ(name="task1-queue", visibility_timeout=100, dlq=dlq)
    task2_queue = OpenQ(name="task2-queue", visibility_timeout=100, dlq=dlq)

    # Set number of tasks to be produced
    num_tasks = 5

    # Using threading to simulate concurrent producing and consuming processes
    producer_thread = Producer(task1_queue, num_tasks)
    task1_consumer_thread = Task1Consumer(task1_queue)
    task2_consumer_thread = Task2Consumer(task2_queue)

    try:
        producer_thread.start()
        task1_consumer_thread.start()
        task2_consumer_thread.start()

        # You should also take care of stopping the consumers properly when done.
        # Here's the simplified way to join them (in real world applications you
        # would need to manage thread lifecycle more carefully):

        producer_thread.join()
        task1_consumer_thread.stop()
        task2_consumer_thread.stop()
        task1_consumer_thread.join()
        task2_consumer_thread.join()

    except KeyboardInterrupt:
        logging.info("Terminating the threads...")
        task1_consumer_thread.stop()
        task2_consumer_thread.stop()
        task1_consumer_thread.join()
        task2_consumer_thread.join()
        producer_thread.join()
