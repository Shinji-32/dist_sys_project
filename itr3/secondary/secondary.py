import flask
import grpc
import logging
import os
import threading
import random
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
        msg_id, message = request.message.split(":", 1)
        msg_id = int(msg_id)
        log.info(f"Отримано повідомлення для реплікації: {message} з id {msg_id}")

        if any(m[0] == msg_id for m in messages):
            log.info(f"Повідомлення з id {msg_id} уже існує, пропускаємо")
            return replication_pb2.AckResponse(success=True)

        if random.random() < 0.1: 
            log.error(f"Симуляція внутрішньої помилки для повідомлення {msg_id}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Симульована внутрішня помилка")
            raise grpc.RpcError("Симульована внутрішня помилка")

        time.sleep(random.uniform(5, 10))  
        messages.append((msg_id, message))
        return replication_pb2.AckResponse(success=True)

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    log.info(f"gRPC сервер запущено на порту {GRPC_PORT}")
    server.start()
    server.wait_for_termination()

@app.route("/messages", methods=["GET"])
def list_messages():
    
    sorted_messages = sorted(messages, key=lambda x: x[0])
    display_messages = []
    expected_id = 0
    for msg_id, msg in sorted_messages:
        if msg_id == expected_id:
            display_messages.append(msg)
            expected_id += 1
        else:
            break  
    log.info(f"Список реплікованих повідомлень: {display_messages}")
    return flask.jsonify({"messages": display_messages}), 200

if __name__ == "__main__":
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()
    try:
        with grpc.insecure_channel("master:50050") as channel:
            stub = replication_pb2_grpc.ReplicationServiceStub(channel)
            stub.ReplicateMessage(replication_pb2.MessageRequest(message=f"0:SYNC_{GRPC_PORT}"))
    except grpc.RpcError as e:
        log.error(f"Не вдалося повідомити майстра про запуск: {e}")
    app.run(host="0.0.0.0", port=HTTP_PORT)
