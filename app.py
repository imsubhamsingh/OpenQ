import sys
import time
from uuid import uuid4
import logging
import threading
from openq.openq import OpenQ, Producer, Consumer
from openq.utils import is_redis_running
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# Add your existing classes and functions here: Message, OpenQ etc.


@app.route("/")
def index():
    return render_template(
        "index.html"
    )  # You would create an index.html template file in a 'templates' directory


@app.route("/messages/<queue_name>", methods=["GET"])
def get_messages(queue_name):
    queue = OpenQ(name=queue_name)
    messages = queue.get_messages(queue_name)
    return jsonify(messages)


# A simple in-memory structure to store queues
queues = {}


@app.route("/create-queue", methods=["POST"])
def create_queue():
    # Extract queue name from request payload
    data = request.json
    queue_name = data.get("queueName")

    # Check if queue already exists
    if queue_name in queues:
        return jsonify(error="Queue already exists"), 400

    # Create a new queue and assign a unique identifier
    queue_id = str(uuid4())
    queues[queue_name] = {
        "id": queue_id,
        "messages": [],
        "created_at": time.time(),
        "consumed_messages": 0,
    }

    # Return success response
    return jsonify(queueId=queue_id), 201


@app.route("/enqueue-message", methods=["POST"])
def enqueue_message():
    data = request.json
    queue_name = data["queueName"]
    message_body = data["messageBody"]

    if queue_name not in queues:
        return jsonify(error="Queue not found."), 404

    message_id = str(uuid4())
    message = {"id": message_id, "body": message_body, "received_at": time.time()}
    queues[queue_name]["messages"].append(message)

    return jsonify(messageId=message_id), 201


@app.route("/dequeue-message", methods=["POST"])
def dequeue_message():
    data = request.json
    queue_name = data["queueName"]

    if queue_name not in queues:
        return jsonify(error="Queue not found."), 404

    queued_messages = queues[queue_name]["messages"]
    if len(queued_messages) == 0:
        return jsonify(error="No messages to dequeue."), 200

    message = queued_messages.pop(0)
    queues[queue_name]["consumed_messages"] += 1

    return jsonify(messageId=message["id"], messageBody=message["body"]), 200


@app.route("/queue-attributes", methods=["GET"])
def get_queue_attributes():
    queue_name = request.args.get("queueName")

    if queue_name not in queues:
        return jsonify(error="Queue not found."), 404

    queue_info = queues[queue_name]
    num_available_messages = len(queue_info["messages"])
    num_consumed_messages = queue_info["consumed_messages"]

    return jsonify(
        queueId=queue_info["id"],
        created_at=queue_info["created_at"],
        availableMessages=num_available_messages,
        consumedMessages=num_consumed_messages,
    ), 200


@app.route("/queue-list")
def list_queues():
    # Perform actual data fetching here from your queue service or database.
    return jsonify(queues), 200


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

    # Create main queue and dead-letter queue if they don't exist yet
    dlq = OpenQ(name="dead-letter-queue", visibility_timeout=100)
    main_queue = OpenQ(name="main-queue", visibility_timeout=100, dlq=dlq)

    # Start the Flask app on a different thread
    threading.Thread(target=lambda: app.run(debug=True, use_reloader=False)).start()

    # Set number of tasks to be produced
    num_tasks = 5

    # Using threading to simulate concurrent producing and consuming processes
    producer_thread = Producer(main_queue, num_tasks)
    consumer_thread = Consumer(main_queue)

    try:
        producer_thread.start()
        consumer_thread.start()

        producer_thread.join()  # Wait for the producer to finish
        consumer_thread.stop()  # Stop the consumer gracefully
        consumer_thread.join()  # Wait for the consumer to finish

    except KeyboardInterrupt:
        logging.info("Terminating the threads...")
        consumer_thread.stop()
        consumer_thread.join()
        producer_thread.join()
