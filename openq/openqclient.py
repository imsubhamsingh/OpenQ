import time
import logging
from openq import OpenQ
from redis import Redis, ConnectionPool, RedisError

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenQClient:
    def __init__(self, host="localhost", port=6379, db=0):
        # Use connection pool for better performance and thread-safety
        self.redis_client = Redis(
            connection_pool=ConnectionPool(host=host, port=port, db=db)
        )

    def execute_with_retry(self, func, *args, **kwargs):
        retries = 3
        backoff_factor = 0.3
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except (RedisError, ConnectionError) as e:
                wait = backoff_factor * (2**attempt)
                logger.error(f"Operation failed: {e}. Retrying in {wait} seconds...")
                if attempt + 1 < retries:
                    time.sleep(wait)
                else:
                    logger.error("Maximum retries reached")
                    raise

    def create_queue(self, queue_name, visibility_timeout=300):
        return self.execute_with_retry(
            OpenQ.create_queue,
            queue_name,
            visibility_timeout=visibility_timeout,
            redis_client=self.redis_client,
        )

    def delete_queue(self, queue_name):
        return self.execute_with_retry(
            OpenQ.delete_queue, queue_name, redis_client=self.redis_client
        )

    def list_queues(self):
        return self.execute_with_retry(
            OpenQ.list_queues, redis_client=self.redis_client
        )

    def enqueue_message(self, queue_name, message_body):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return self.execute_with_retry(queue.enqueue, message_body)

    def dequeue_message(self, queue_name):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return self.execute_with_retry(
            queue.dequeue,
        )

    def acknowledge_message(self, queue_name, message_id):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return self.execute_with_retry(queue.acknowledge, queue_name, message_id)


# # Example usage:
# if __name__ == "__main__":
#     client = OpenQClient()  # assumes Redis running on localhost:6379
#     queue_name = "sample-queue"

#     # Create a queue
#     client.create_queue(queue_name)

#     # Enqueue a message
#     msg_id = client.enqueue_message(queue_name, "Hello OpenQ!")

#     # Dequeue a message
#     message = client.dequeue_message(queue_name)

#     # Acknowledge a message
#     if message:
#         client.acknowledge_message(queue_name, message.id)

#     # List queues
#     queues = client.list_queues()
#     print(queues)

#     # Delete a queue
#     client.delete_queue(queue_name)
#     print("DONE")
