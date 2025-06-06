from flask import Flask, request, jsonify
import logging
import time
import threading
import random
import requests
lock = threading.Lock()

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# item example: { "id": str, "message": str, "order": int }
messages = []

buffered = {}
expected_order = 0

MASTER_URL = "http://master:5000"


def attempt_initial_sync():
    global messages, expected_order
    try:
        resp = requests.get(f"{MASTER_URL}/full_messages", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            all_msgs = data.get("messages", [])
            all_msgs_sorted = sorted(all_msgs, key=lambda x: x["order"])
            for entry in all_msgs_sorted:
                messages.append({
                    "id": entry["id"],
                    "message": entry["message"],
                    "order": entry["order"]
                })
            expected_order = len(messages)
            logger.info(f"[SECONDARY] Initial sync: fetched {expected_order} messages from master.")
        else:
            logger.warning(f"[SECONDARY] Initial sync: master returned {resp.status_code}")
    except Exception as e:
        logger.error(f"[SECONDARY] Initial sync error: {e} (master may be unavailable)")


def try_buffered_delivery():
    global messages, buffered, expected_order
    while expected_order in buffered:
        entry = buffered.pop(expected_order)
        messages.append(entry)
        logger.info(f"[SECONDARY] Buffered message order={entry['order']} delivered from buffer.")
        expected_order += 1


@app.route('/replicate', methods=['POST'])
def replicate_message():
    global messages, buffered, expected_order

    data = request.get_json()
    if not data or 'id' not in data or 'message' not in data or 'order' not in data:
        return jsonify({"error": "ID, message и order обязательны"}), 400

    msg_id = data["id"]
    msg_text = data["message"]
    order = data["order"]

    if any(m["order"] == order or m["id"] == msg_id for m in messages) or order in buffered:
        logger.warning(f"[SECONDARY] Duplicate message id={msg_id}, order={order}. Ignoring, but returning ACK.")
        return jsonify({"status": "ACK"}), 200

    entry = {"id": msg_id, "message": msg_text, "order": order}
    if order == expected_order:
        time.sleep(1)  # response delay
        messages.append(entry)
        logger.info(f"[SECONDARY] Received message order={order}, id={msg_id}.")
        expected_order += 1
        try_buffered_delivery()
    else:
        buffered[order] = entry
        logger.info(f"[SECONDARY] Message order={order} buffered (waiting for {expected_order}).")

    return jsonify({"status": "ACK"}), 200


@app.route('/messages', methods=['GET'])
def get_messages():
    sorted_msgs = sorted(messages, key=lambda x: x["order"])
    return jsonify({"messages": [m["message"] for m in sorted_msgs]}), 200


@app.route('/clear', methods=['POST'])
def clear_secondary():
    global messages, buffered, expected_order
    with lock:
        messages.clear()
        buffered.clear()
        expected_order = 0
    logger.info("[SECONDARY] State cleared via HTTP /clear")
    return jsonify({"status": "cleared"}), 200


def run_secondary():
    attempt_initial_sync()
    app.run(host='0.0.0.0', port=5001, threaded=True)


if __name__ == '__main__':
    run_secondary()
