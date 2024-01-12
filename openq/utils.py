import subprocess
import logging
from json import JSONEncoder
from datetime import datetime
from uuid import UUID


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            # Format the date to a string that JSON can serialize
            return obj.isoformat()

        if isinstance(obj, UUID):
            return str(obj)

        return JSONEncoder.default(self, obj)


def is_redis_running():
    try:
        output = subprocess.check_output(["redis-cli", "ping"])
        return output.strip() == b"PONG"
    except Exception as e:
        logging.warning(f"❌ Checking Redis server failed: {e}")
        return False
