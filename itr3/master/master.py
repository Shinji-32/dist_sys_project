import flask
import grpc
import logging
import threading
import random
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
from queue import Queue

app = flask.Flask(__name__)
messages = []  
message_id = 0
secondary_addresses = ["secondary1:50051", "secondary2:50052"]
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


pending_messages = {addr: Queue() for addr in secondary_addresses}
last_acked_message = {addr: -1 for addr in secondary_addresses}  

class ReplicationServiceServicer(replication_pb2_grpc.ReplicationServiceServicer):
    def ReplicateMessage(self, request, context):
        global message_id
        log.info(f"Майстер отримав повідомлення для реплікації: {request.message}")
        time.sleep(random.uniform(1, 3))
        msg_id, message = request.message.split(":", 1)
        msg_id = int(msg_id)

        if message.startswith("SYNC_"):
            addr = f"secondary{msg_id+1}:{message.split('_')[1]}"  
            log.info(f"Отримано SYNC від {addr}, запускаємо синхронізацію")
            sync_missing_messages(addr)
            process_pending_messages(addr)
            return replication_pb2.AckResponse(success=True)

        if not any(m[0] == msg_id for m in messages):
            messages.append((msg_id, message))
        return replication_pb2.AckResponse(success=True)

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationServiceServicer(), server)
    server.add_insecure_port("[::]:50050")
    log.info("Майстер gRPC сервер запущено на порту 50050")
    server.start()
    server.wait_for_termination()

def replicate_to_secondary(addr, message, msg_id, timeout=10, max_attempts=5):
    attempt = 1
    while attempt <= max_attempts:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = replication_pb2_grpc.ReplicationServiceStub(channel)
                log.info(f"Надсилання повідомлення {message} з id {msg_id} до {addr} (спроба {attempt})")
                response = stub.ReplicateMessage(
                    replication_pb2.MessageRequest(message=f"{msg_id}:{message}"),
                    timeout=timeout
                )
                log.info(f"Отримано ACK від {addr} для повідомлення {message}")
                last_acked_message[addr] = max(last_acked_message[addr], msg_id)
                return response.success
        except grpc.RpcError as e:
            log.error(f"Не вдалося реплікувати до {addr}: {e}")
            if attempt == max_attempts:
                log.error(f"Досягнуто максимальної кількості спроб ({max_attempts}) для {addr}")
                return False
            time.sleep(min(2 ** attempt, 10))  
            attempt += 1
    return False

def sync_missing_messages(addr):
    """Надсилає пропущені повідомлення до вторинного вузла після його відновлення."""
    last_acked = last_acked_message[addr]
    for msg_id, msg in sorted(messages):
        if msg_id > last_acked:
            log.info(f"Синхронізація пропущеного повідомлення {msg} з id {msg_id} до {addr}")
            pending_messages[addr].put((msg_id, msg))

def process_pending_messages(addr):
    """Обробляє черги пропущених повідомлень для вторинного вузла."""
    while not pending_messages[addr].empty():
        msg_id, message = pending_messages[addr].get()
        if replicate_to_secondary(addr, message, msg_id):
            pending_messages[addr].task_done()
        else:
            pending_messages[addr].put((msg_id, message)) 
            break

@app.route("/messages", methods=["POST"])
def append_message():
    global message_id
    data = flask.request.json
    log.info(f"Отримано запит: {flask.request.data}")
    try:
        message = data.get("message")
        w = min(int(data.get("w", 1)), len(secondary_addresses) + 1)
    except Exception as e:
        log.error(f"Помилка розбору JSON: {e}")
        return flask.jsonify({"error": "Некоректний JSON"}), 400

    if not message:
        return flask.jsonify({"error": "Не вказано повідомлення"}), 400

    messages.append((message_id, message))
    log.info(f"Додано повідомлення: {message} з id {message_id} та w={w}")


    for addr in secondary_addresses:
        pending_messages[addr].put((message_id, message))


    threads = []
    for addr in secondary_addresses:
        t = threading.Thread(target=process_pending_messages, args=(addr,))
        t.start()
        threads.append(t)


    start_time = time.time()
    required_acks = w
    ack_count = 1 
    while ack_count < required_acks and time.time() - start_time < 60:  
        for addr in secondary_addresses:
            if last_acked_message[addr] >= message_id:
                ack_count += 1
        if ack_count < required_acks:
            log.info(f"Очікуємо {required_acks} ACK, отримано {ack_count}")
            time.sleep(0.5) 
        else:
            break

    if ack_count >= required_acks:
        log.info(f"Отримано {ack_count} ACK, потрібно {required_acks}, успішно")
        message_id += 1  
        return flask.jsonify({"status": "success", "messages": [msg[1] for msg in sorted(messages)]}), 200
    else:
        log.error(f"Не отримано достатньо ACK: отримано {ack_count}, потрібно {required_acks}")
        messages.pop()  
        return flask.jsonify({"error": "Недостатньо ACK"}), 500

@app.route("/messages", methods=["GET"])
def list_messages():
    log.info(f"Список повідомлень: {[msg[1] for msg in sorted(messages)]}")
    return flask.jsonify({"messages": [msg[1] for msg in sorted(messages)]}), 200

@app.route("/sync/<addr>", methods=["POST"])
def sync_node(addr):
    """Синхронізація вторинного вузла після його відновлення."""
    if addr not in secondary_addresses:
        return flask.jsonify({"error": "Невідомий вторинний вузол"}), 400
    log.info(f"Синхронізація вузла {addr}")
    sync_missing_messages(addr)
    process_pending_messages(addr)
    return flask.jsonify({"status": "success"}), 200

if __name__ == "__main__":
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=5000)
