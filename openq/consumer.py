import time
import logging
import threading
from openq import OpenQ

DEBUG = True


class OpenQConsumer(threading.Thread):
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
            # Acknowledge and delete the message from the queue after processing
            self.queue.acknowledge(message.id)
            logging.info(
                f"✅ Successfully processed and acknowledged Task ID: {message.id}"
            )
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


def run_consumer():
    # Configure Queue here
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="main-queue", visibility_timeout=100, dlq=dlq)

    consumer_thread = OpenQConsumer(main_queue)

    consumer_thread.start()
    # consumer_thread.stop()  # This will be needed to stop the consumer cleanly later
    consumer_thread.join()


if __name__ == "__main__":
    run_consumer()
