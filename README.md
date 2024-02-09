# OpenQ - Open source Messaging Queue Service

OpenQ is a sophisticated, Python-based in-memory FIFO (First-In-First-Out) message queue service that is engineered for simplicity and high performance. The application acts as a middleware that enables effective message exchange within distributed systems, ensuring reliable communication with scalability features.

## Features

- **In-Memory Queueing**: Facilitates rapid access and low latency processing by storing messages directly in memory.
- **FIFO Message Delivery**: Maintains order integrity by ensuring messages are processed in the same order they were received.
- **Visibility Timeouts**: Prevents message duplication by making them 'invisible' after being dequeued for a specified timeout period.
- **Dead-Letter Queue (DLQ)**: Provides a mechanism to isolate messages that fail to process correctly for further analysis or retry.
- **Thread Safety**: Incorporates threading locks to secure concurrent access to the message queues.
- **Persistence Capabilities**: Enables saving and loading the state of the queues to ensure data durability across system restarts.
- **Automatic Message Retries**: Re-introduces messages to the queue after a failed attempt due to expiration of the visibility timeout.

## Design

OpenQ consists of two pivotal components:

1. **Message Class**: Encapsulates details such as unique identifiers, message content, and timestamps relevant to each message.
2. **OpenQ Class**: Manages the core operations including enqueueing, dequeueing, tracking visibility timeouts, and handling a dead-letter queue.

## Message Queue Architecture Using Two Separate Queues

OpenQ employs two distinct queues to manage messages at different stages of the processing lifecycle:

1. **Processing Queue**: This queue holds all incoming messages waiting to be processed. Messages are enqueued here when produced by a producer and dequeued by consumers for processing.

2. **Consumed Messages Queue**: Post-dequeue, messages are temporarily moved to this queue until they are acknowledged after successful processing. If a consumer fails to process a message before the visibility timeout expires, the message can be re-enqueued into the processing queue or moved to the dead-letter queue.

The separation of concerns provided by using two queues offers several benefits:

- **Reliability**: Ensures that messages are not lost during processing. If a consumer crashes mid-processing, the unacknowledged message remains in the consumed messages queue and can be recovered.
- **Accountability**: Provides a way to track which messages are currently "in-flight" or being processed, helping identify bottlenecks or errors in the system.
- **Efficiency**: Improves throughput as producers and consumers operate on different data structures without interfering with each other's operations.
- **Fault Tolerance**: By keeping the unprocessed and processed messages in different queues, we can recover from failures effectively. If a consumer dies after pulling a message from the Processing Queue but before processing, the message remains in the Consumed Messages Queue for a retry or further examination.
- **Visibility and Tracking**: Separating the queues allow us to easily identify where each message is in its lifecycle; whether it's waiting to be processed, currently being processed, or has finished processing.
- **Concurrency Control**: With two separate queues, the system can function more efficiently by allowing producers to add to the processing queue while consumers work on the consumed messages without blocking each other.

Separate threads for producers and consumers simulate an asynchronous environment akin to real-life processing scenarios.

## Time and Space Complexities of Deque Data Structure

A Deque (double-ended queue) is an abstract data type with capabilities to add or remove elements from both ends with an average performance of O(1).

### Time Complexities:
- **Insertion** (`append`, `appendleft`): O(1)
- **Deletion** (`pop`, `popleft`): O(1)
- **Peeking** (accessing the element at the front or rear end): O(1)

Because these operations do not require shifting all other elements in the data structure (unlike arrays or single-ended queues), deques provide excellent performance for queue-related operations.

### Space Complexities:
- **Space Complexity**: The space complexity for a deque is O(n), where n is the number of items in the deque. This is because each item takes up a fixed amount of space and the total space required grows linearly with the number of items.

Since the deque allows us to insert and delete at both ends at constant time, it is particularly suitable for our queues in the message-processing scenario:

- For the **Processing Queue**, we need fast removals and insertions as messages frequently come in and get picked up for processing.
- For the **Consumed Messages Queue**, we need fast deletes once a message is acknowledged, ensuring high throughput and minimal latency.

The combination of these optimized operations ensures that the system can quickly adjust to varying workloads, prioritize tasks, and maintain stability under load, all the while providing high availability and reliability for the messages being handled.


### Message Lifecycle

1. **Production**: The producer creates messages that enter the processing queue.
2. **Consumption**: Consumers pick these messages from the processing queue, at which point they move to the consumed messages queue pending acknowledgment after processing.
3. **Acknowledgment**: Post-processing, the messages are acknowledged, which removes them from the consumed messages queue permanently.
4. **Failure Handling**: If a message is not acknowledged within the visibility timeout, it gets requeued to the processing queue or moved to the dead-letter queue.

## Usage

To start utilizing OpenQ, initialize both the main and dead-letter queues. Here’s an example setup:

```python
from OpenQ import OpenQ, Producer, Consumer

# Initialize the dead-letter queue and the main queue
dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
main_queue = OpenQ(name="main-queue", visibility_timeout=100, dlq=dlq)

# Create and start producer and consumer threads
producer_thread = Producer(main_queue, num_tasks=10)
consumer_thread = Consumer(main_queue)

producer_thread.start()
consumer_thread.start()

# Ensure the completion of the threads
producer_thread.join()
consumer_thread.join()
```

Invoke `stop()` on the `Consumer` instance for a smooth shutdown if required.

Please modify various parameters like `num_tasks`, visibility timeouts, etc., according to your specific workload demands.

## Getting Started

Clone the repository, install any dependencies from `requirements.txt`, and execute the sample script to see OpenQ in operation. Before deploying OpenQ in a production scenario, it is recommended to test extensively and configure persistence and monitoring systems for optimal queue management and troubleshooting.

---

*Important: OpenQ is crafted for educational and demonstrational purposes. It should undergo stringent testing and modifications before any production-level deployment.*
