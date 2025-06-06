import flask
import grpc
import logging
import threading
import random
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc

app = flask.Flask(__name__)
messages = []  
message_id = 0
secondary_addresses = ["secondary1:50051", "secondary2:50052"]
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

class ReplicationServiceServicer(replication_pb2_grpc.ReplicationServiceServicer):
    def ReplicateMessage(self, request, context):
        global message_id
        log.info(f"Replicating message: {request.message}")
        time.sleep(random.uniform(1, 3))
        messages.append((message_id, request.message))
        message_id += 1
        return replication_pb2.AckResponse(success=True)

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationServiceServicer(), server)
    server.add_insecure_port("[::]:50050")
    server.start()
    server.wait_for_termination()

def replicate_to_secondary(addr, message, msg_id):
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = replication_pb2_grpc.ReplicationServiceStub(channel)
            log.info(f"Sending message {message} with id {msg_id} to {addr}")
            response = stub.ReplicateMessage(replication_pb2.MessageRequest(message=f"{msg_id}:{message}"))
            log.info(f"Received ACK from {addr} for message {message}")
            return response.success
    except grpc.RpcError as e:
        log.error(f"Failed to replicate to {addr}: {e}")
        return False

@app.route("/messages", methods=["POST"])
def append_message():
    global message_id
    data = flask.request.json
    message = data.get("message")
    w = min(int(data.get("w", 1)), len(secondary_addresses) + 1)  # w від 1 до n+1
    if not message:
        return flask.jsonify({"error": "No message provided"}), 400
    messages.append((message_id, message))
    message_id += 1
    log.info(f"Appending message: {message} with w={w}")

    
    def replicate():
        threads = [threading.Thread(target=replicate_to_secondary, args=(addr, message, message_id-1)) for addr in secondary_addresses]
        for t in threads:
            t.start()
        return [t for t in threads]

    threads = replicate()
    ack_count = 1  
    for t in threads[:w-1]:
        t.join()
        if replicate_to_secondary(secondary_addresses[threads.index(t)], message, message_id-1):
            ack_count += 1

    if ack_count >= w:
        log.info(f"Received {ack_count} ACKs, required {w}, proceeding")
        return flask.jsonify({"status": "success", "messages": [msg[1] for msg in sorted(messages)]}), 200
    else:
        log.error(f"Failed to receive enough ACKs: got {ack_count}, required {w}")
        return flask.jsonify({"error": "Not enough ACKs"}), 500

@app.route("/messages", methods=["GET"])
def list_messages():
    log.info(f"Listing messages: {[msg[1] for msg in sorted(messages)]}")
    return flask.jsonify({"messages": [msg[1] for msg in sorted(messages)]}), 200

if __name__ == "__main__":
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000)