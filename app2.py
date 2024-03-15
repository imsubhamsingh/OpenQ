from flask import Flask, request, jsonify
from openq import OpenQ  # Assuming your code is in openq.py.
import logging

# Create Flask app instance
app = Flask(__name__)

# Initialize OpenQ (use actual Redis configuration)
main_queue = OpenQ(name="main-queue")


@app.route("/")
def index():
    return "OpenQ"


@app.route("/queues/all")
def get_all_queues():
    queues = OpenQ.list_queues()
    indexed_queues = [{"index": i + 1, "name": queue} for i, queue in enumerate(queues)]
    return jsonify(indexed_queues)


@app.route("/<queue_name>/messages>", methods=["GET"])
def get_messages(queue_name):
    queue = OpenQ(name=queue_name)
    messages = queue.get_messages()
    return jsonify(messages)


@app.route("/create-queue/<queue_name>", methods=["POST"])
def create_queue(queue_name):
    # queue_name = request.json.get('name')
    visibility_timeout = request.json.get("visibility_timeout", 300)

    try:
        OpenQ.create_queue(queue_name, visibility_timeout=visibility_timeout)
        return jsonify({"message": f"Queue {queue_name} created successfully"}), 201
    except Exception as e:
        logging.error(f"Failed to create queue '{queue_name}': {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/queues/<queue_name>/delete", methods=["DELETE"])
def delete_queue(queue_name):
    try:
        OpenQ.delete_queue(queue_name)
        return jsonify({"message": f"Queue {queue_name} deleted successfully"}), 200
    except Exception as e:
        logging.error(f"Failed to delete queue '{queue_name}': {e}")
        return jsonify({"error": str(e)}), 404


@app.route("/queues/<queue_name>/enqueue", methods=["POST"])
def enqueue_message(queue_name):
    message_body = request.json.get("body")

    try:
        message_id = main_queue.enqueue(message_body)
        return jsonify({"message_id": message_id}), 200
    except Exception as e:
        logging.error(f"Error on message enqueue: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/queues/<queue_name>/dequeue", methods=["GET"])
def dequeue_message(queue_name):
    try:
        message = main_queue.dequeue()
        if message:
            return jsonify(message.__dict__), 200
        else:
            return jsonify({"error": "Queue is empty"}), 404
    except Exception as e:
        logging.error(f"Error on message dequeue: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/queues/<queue_name>/messages/<message_id>", methods=["DELETE"])
def acknowledge_message(queue_name, message_id):
    try:
        main_queue.acknowledge(message_id)
        return jsonify({"message": f"Message {message_id} acknowledged"}), 200
    except Exception as e:
        logging.error(f"Error on message acknowledge: {e}")
        return jsonify({"error": str(e)}), 404


@app.route("/queues/<queue_name>/purge", methods=["POST"])
def purge_queue(queue_name):
    try:
        main_queue.purge_queue()
        return jsonify({"message": f"Queue {queue_name} purged successfully"}), 200
    except Exception as e:
        logging.error(f"Error on queue purge: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
