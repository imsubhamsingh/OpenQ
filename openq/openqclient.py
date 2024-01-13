import logging
from openq import OpenQ
from redis import Redis

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenQClient:
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis_client = Redis(host=host, port=port, db=db)

    def create_queue(self, queue_name, visibility_timeout=300):
        return OpenQ.create_queue(
            queue_name,
            visibility_timeout=visibility_timeout,
            redis_client=self.redis_client,
        )

    def delete_queue(self, queue_name):
        return OpenQ.delete_queue(queue_name, redis_client=self.redis_client)

    def list_queues(self):
        return OpenQ.list_queues(redis_client=self.redis_client)

    def enqueue_message(self, queue_name, message_body):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return queue.enqueue(message_body)

    def dequeue_message(self, queue_name):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return queue.dequeue()

    def acknowledge_message(self, queue_name, message_id):
        queue = OpenQ(name=queue_name, redis_client=self.redis_client)
        return queue.acknowledge(message_id)


# Example usage:
if __name__ == "__main__":
    client = OpenQClient()  # assumes Redis running on localhost:6379
    queue_name = "sample-queue"

    # Create a queue
    client.create_queue(queue_name)

    # Enqueue a message
    msg_id = client.enqueue_message(queue_name, "Hello OpenQ!")

    # Dequeue a message
    message = client.dequeue_message(queue_name)

    # Acknowledge a message
    if message:
        client.acknowledge_message(queue_name, message.id)

    # List queues
    queues = client.list_queues()
    print(queues)

    # Delete a queue
    client.delete_queue(queue_name)
    print("DONE")
