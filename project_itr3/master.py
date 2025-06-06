from flask import Flask, request, jsonify
import logging
import time
import uuid
import threading
import requests
lock = threading.Lock()

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# item example: { "id": ..., "message": ..., "order": ... }
messages = []

SECONDARIES = [
    'http://secondary1:5001',
    'http://secondary2:5001'
]

def sync_replicate_to_secondary(secondary_url, message_entry):
    url = f"{secondary_url}/replicate"
    payload = {
        "id": message_entry["id"],
        "message": message_entry["message"],
        "order": message_entry["order"]
    }
    while True:
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                logger.info(f"ACK from {secondary_url} for message {message_entry['id']} (order={message_entry['order']})")
                return True
            else:
                logger.warning(f"Non-200 from {secondary_url}: {resp.status_code}, retrying...")
        except Exception as e:
            logger.error(f"Error replicating to {secondary_url}: {e}, retrying in 2s...")
        time.sleep(2)


def async_replicate_to_secondary(secondary_url, message_entry):
    def worker():
        sync_replicate_to_secondary(secondary_url, message_entry)
    t = threading.Thread(target=worker, daemon=True)
    t.start()


@app.route('/messages', methods=['POST'])
def post_message():
    data = request.get_json()
    if not data or 'message' not in data or 'w' not in data:
        return jsonify({"error": "Required 'message' and 'w'"}), 400

    message_text = data['message']
    w = data['w']

    max_w = len(SECONDARIES) + 1
    if not isinstance(w, int) or w < 1 or w > max_w:
        return jsonify({"error": f"Write concern w must be in range (1, {max_w})"}), 400

    message_id = str(uuid.uuid4())
    order = len(messages)
    message_entry = {
        "id": message_id,
        "message": message_text,
        "order": order
    }

    messages.append(message_entry)
    logger.info(f"[MASTER] Saved message «{message_text}» id={message_id}, order={order}")

    if w == 1:
        for sec in SECONDARIES:
            async_replicate_to_secondary(sec, message_entry)
        return jsonify({"status": "ok", "message_id": message_id, "order": order}), 200

    need_sync = SECONDARIES[: w - 1]
    need_async = SECONDARIES[w - 1:]

    for sec_url in need_sync:
        logger.info(f"[MASTER] Waiting ACK from {sec_url} (w-sync)")
        sync_replicate_to_secondary(sec_url, message_entry)

    logger.info(f"[MASTER] Recieved {w} ACK → return OK")
    response = {
        "status": "ok",
        "message_id": message_id,
        "order": order
    }

    for sec_url in need_async:
        async_replicate_to_secondary(sec_url, message_entry)

    return jsonify(response), 200


@app.route('/messages', methods=['GET'])
def get_messages():
    sorted_msgs = sorted(messages, key=lambda x: x["order"])
    return jsonify({"messages": [m["message"] for m in sorted_msgs]}), 200


@app.route('/full_messages', methods=['GET'])
def full_messages():
    sorted_msgs = sorted(messages, key=lambda x: x["order"])
    return jsonify({"messages": sorted_msgs}), 200


@app.route('/clear', methods=['POST'])
def clear_master():
    messages.clear()
    logger.info("[MASTER] Messages cleaned")
    return jsonify({"status": "cleared"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
