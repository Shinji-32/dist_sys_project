import flask
import grpc
import logging
import os
import threading
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc

app = flask.Flask(__name__)
messages = []
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


HTTP_PORT = int(os.getenv("HTTP_PORT", 5001))
GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

class ReplicationServiceServicer(replication_pb2_grpc.ReplicationServiceServicer):
    def ReplicateMessage(self, request, context):
        log.info(f"Received message for replication: {request.message}")
        if request.message not in messages:  
            time.sleep(2)  
            messages.append(request.message)
        return replication_pb2.AckResponse(success=True)

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    log.info(f"gRPC server started on port {GRPC_PORT}")
    server.start()
    server.wait_for_termination()

@app.route("/messages", methods=["GET"])
def list_messages():
    log.info(f"Listing replicated messages: {messages}")
    return flask.jsonify({"messages": messages}), 200

if __name__ == "__main__":
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=HTTP_PORT)