import logging
from openq import OpenQ
from producer import OpenQProducer
from consumer import OpenQConsumer


def run_producer():
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="sample-queue", visibility_timeout=100, dlq=dlq)

    num_tasks = 5

    producer_thread = OpenQProducer(main_queue, num_tasks)

    producer_thread.start()
    producer_thread.join()


def run_consumer():
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="sample-queue", visibility_timeout=100, dlq=dlq)

    consumer_thread = OpenQConsumer(main_queue)

    consumer_thread.start()
    consumer_thread.stop()  # This will be needed to stop the consumer cleanly later
    consumer_thread.join()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Here you would implement some logic or command-line arguments
    # to determine whether to run the producer or consumer.
    run_producer()
    # run_consumer() could also be called here, depending on your requirements
