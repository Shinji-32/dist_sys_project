import flask
import grpc
import logging
import time
import threading
from concurrent import futures
import replication_pb2
import replication_pb2_grpc

app = flask.Flask(__name__)
messages = []
secondary_addresses = ["secondary1:50051", "secondary2:50052"] 
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

class ReplicationServiceServicer(replication_pb2_grpc.ReplicationServiceServicer):
    def ReplicateMessage(self, request, context):
        log.info(f"Replicating message: {request.message}")
        time.sleep(2) 
        return replication_pb2.AckResponse(success=True)

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationServiceServicer(), server)
    server.add_insecure_port("[::]:50050")
    server.start()
    server.wait_for_termination()

@app.route("/messages", methods=["POST"])
def append_message():
    message = flask.request.json.get("message")
    if not message:
        return flask.jsonify({"error": "No message provided"}), 400
    messages.append(message)
    log.info(f"Appending message: {message}")

    def replicate_to_secondary(addr):
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = replication_pb2_grpc.ReplicationServiceStub(channel)
                response = stub.ReplicateMessage(replication_pb2.MessageRequest(message=message))
                return response.success
        except grpc.RpcError as e:
            log.error(f"Failed to replicate to {addr}: {e}")
            return False

    threads = [threading.Thread(target=replicate_to_secondary, args=(addr,)) for addr in secondary_addresses]
    for t in threads:
        t.start()
    for t in threads:
        t.join() 

    if all(replicate_to_secondary(addr) for addr in secondary_addresses):
        return flask.jsonify({"status": "success", "messages": messages}), 200
    else:
        return flask.jsonify({"error": "Failed to replicate to all secondaries"}), 500

@app.route("/messages", methods=["GET"])
def list_messages():
    log.info(f"Listing messages: {messages}")
    return flask.jsonify({"messages": messages}), 200

if __name__ == "__main__":
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000)