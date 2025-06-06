from flask import Flask, request, jsonify
import logging
import time

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

messages = []

@app.route('/replicate', methods=['POST'])
def replicate_message():
    data = request.get_json()
    if not data or 'id' not in data or 'message' not in data or 'order' not in data:
        return jsonify({"error": "ID, message и order requiere"}), 400

    message_id = data['id']
    message = data['message']
    order = data['order']

    time.sleep(8)

    if not any(m.get("id") == message_id for m in messages):
        messages.append({"id": message_id, "message": message, "order": order})
        logger.info(f"[CLIENT] Accepted replication message id={message_id}, order={order}")

    return jsonify({"status": "ACK"}), 200

@app.route('/messages', methods=['GET'])
def get_messages():
    sorted_msgs = sorted(messages, key=lambda x: x["order"])
    return jsonify({"messages": [m["message"] for m in sorted_msgs]}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, threaded=True)
