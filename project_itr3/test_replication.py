import threading
import time
import requests
import pytest
import subprocess

MASTER_URL     = "http://localhost:5000"
SECONDARY1_URL = "http://localhost:5001"
SECONDARY2_URL = "http://localhost:5002"

REQUEST_TIMEOUT = 2.0

CONTAINER_SECONDARY_1 = "45de53b442b2c19a89059d5c81a73b61aa2f52bd7a81a06cf6a4dbfbb36d9b79"
CONTAINER_SECONDARY_2 = "48057e1058076f1a2fec5420208470b16cbdbebea2317bb72b875fac89f485c3"

def clear_all():
    for url in (MASTER_URL, SECONDARY1_URL, SECONDARY2_URL):
        try:
            requests.post(f"{url}/clear", timeout=REQUEST_TIMEOUT)
        except:
            pass
        time.sleep(0.2)
    time.sleep(0.5)


@pytest.fixture(autouse=True)
def ensure_clean_state():
    clear_all()
    yield
    clear_all()


def post_message(master_url: str, text: str, w: int, timeout: float = None):
    return requests.post(f"{master_url}/messages",
                         json={"message": text, "w": w},
                         timeout=timeout)


def get_messages(url: str):
    r = requests.get(f"{url}/messages", timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("messages", [])


# ============================================
# 1) TEST: retry + blocking client when w=3 and secondary2 paused
# ============================================
def test_retry_and_blocking_for_w3():
    """
    1. /clean
    2. docker pause
    3. blocking check (w=3)
    4. master save MsgX 
    5. check (w=1)
    6. docker unpause + w=3 unblock
    7. messages check
    """

    # 2.
    subprocess.run(["docker", "pause", CONTAINER_SECONDARY_2], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    results = {}

    def background_post():
        t0 = time.time()
        try:
            r = post_message(MASTER_URL, "MsgX", w=3, timeout=30)
            results["code"] = r.status_code
        except Exception as e:
            results["error"] = str(e)
        results["elapsed"] = time.time() - t0

    # 3.
    thread = threading.Thread(target=background_post)
    thread.start()

    # 4.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            msgs = get_messages(MASTER_URL)
            if "MsgX" in msgs:
                break
        except:
            pass
        time.sleep(0.2)
    else:
        pytest.fail("Master not saved MsgX in 5 seconds")

    # 5.
    t1 = time.time()
    r_quick = post_message(MASTER_URL, "MsgQuick", w=1, timeout=REQUEST_TIMEOUT)
    t2 = time.time()
    assert r_quick.status_code == 200
    assert (t2 - t1) < REQUEST_TIMEOUT, "w=1 should return instantly"

    # 6.
    subprocess.run(["docker", "unpause", CONTAINER_SECONDARY_2], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(5)

    thread.join(timeout=15)
    assert not thread.is_alive(), "Background w=3 request failed after unpause secondary2"

    assert "error" not in results, f"Background returned error: {results.get('error')}"
    assert results.get("code", 0) == 200, f"Background code != 200: {results.get('code')}"

    # 7.
    time.sleep(1)

    master_msgs = get_messages(MASTER_URL)
    assert set(master_msgs) == {"MsgX", "MsgQuick"}, f"Master wait {{'MsgX','MsgQuick'}}, but get {master_msgs}"

    sec1 = get_messages(SECONDARY1_URL)
    assert set(sec1) == {"MsgX", "MsgQuick"}, f"Secondary1 waiting two, but get {sec1}"

    sec2 = get_messages(SECONDARY2_URL)
    assert set(sec2) == {"MsgX", "MsgQuick"}, f"Secondary2 waiting two, but get {sec2}"


# ============================================
# 2) TEST: secondary2 pulls up missed records at initial sync
# ============================================
def test_missed_messages_after_rejoin():
    """
    1. /clean
    2. docker pause secondary2
    3. POST (A, w=2), POST (B, w=2)
    4. check secondary1 → ['A','B']
    5. GET /messages secondary2 timeout
    6. docker unpause secondary2 + sync ['A','B']
    7. check secondary2 /messages → ['A','B']
    """

    # 2.
    subprocess.run(["docker", "pause", CONTAINER_SECONDARY_2], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    # 3.
    rA = post_message(MASTER_URL, "A", w=2, timeout=REQUEST_TIMEOUT)
    assert rA.status_code == 200
    rB = post_message(MASTER_URL, "B", w=2, timeout=REQUEST_TIMEOUT)
    assert rB.status_code == 200

    # 4.
    deadline = time.time() + 5
    while time.time() < deadline:
        s1 = get_messages(SECONDARY1_URL)
        if s1 == ["A", "B"]:
            break
        time.sleep(1)
    else:
        pytest.fail(f"Secondary1 doesnt contain ['A','B'], but has {get_messages(SECONDARY1_URL)}")

    # 5.
    try:
        requests.get(f"{SECONDARY2_URL}/messages", timeout=3)
    except Exception as e:
        assert 'timeout' in str(e), "Secondary2 not timeout exception"
    else:
        pytest.fail(f"Secondary1 online, should be offline") 

    # 6.
    subprocess.run(["docker", "unpause", CONTAINER_SECONDARY_2], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(5)

    # 7.
    s2 = get_messages(SECONDARY2_URL)
    assert s2 == ["A", "B"], f"Secondary2 doesnt contain ['A','B'], but has {s2}"


# ============================================
# 3) TEST: deduplication in secondary1 (double POST /replicate)
# ============================================
def test_deduplication_on_secondary():
    """
    1. /clean
    2. double POST /replicate to secondary1 with the same {id, message, order}
    3. GET /messages secondary1 → ['X-dedup']
    """
    url_rep = f"{SECONDARY1_URL}/replicate"
    payload = {"id": "dup-1", "message": "X-dedup", "order": 0}

    # 2.
    r1 = requests.post(url_rep, json=payload, timeout=REQUEST_TIMEOUT)
    assert r1.status_code == 200
    time.sleep(0.5)

    r2 = requests.post(url_rep, json=payload, timeout=REQUEST_TIMEOUT)
    assert r2.status_code == 200
    time.sleep(0.5)

    # 3.
    s1 = get_messages(SECONDARY1_URL)
    assert s1 == ["X-dedup"], f"Wait ['X-dedup'], but recieved {s1}"


# ============================================
# 4) TEST: total order + buffering in secondary1
# ============================================
def test_total_order_buffering():
    """
    1. /clean
    2. POST /replicate secondary1:
        - order=0,msg0
        - order=1,msg1
        - order=3,msg3 (msg2 doesnt exist → msg3 buffered)
    3. GET /messages secondary1 → ['msg0','msg1']
    4. POST /replicate order=2,msg2 → buffered
    5. GET /messages secondary1 → ['msg0','msg1','msg2','msg3']
    """
    url_rep = f"{SECONDARY1_URL}/replicate"
    payloads = [
        {"id": "o0", "message": "msg0", "order": 0},
        {"id": "o1", "message": "msg1", "order": 1},
        {"id": "o3", "message": "msg3", "order": 3},
    ]

    # 2.
    for pl in payloads:
        r = requests.post(url_rep, json=pl, timeout=REQUEST_TIMEOUT)
        assert r.status_code == 200
        time.sleep(0.2)

    # 3.
    time.sleep(0.5)
    first = get_messages(SECONDARY1_URL)
    assert first == ["msg0", "msg1"], f"GET: recieved {first}, wait ['msg0','msg1']"

    # 4.
    pl2 = {"id": "o2", "message": "msg2", "order": 2}
    r2 = requests.post(url_rep, json=pl2, timeout=REQUEST_TIMEOUT)
    assert r2.status_code == 200
    time.sleep(0.5)

    # 5.
    second = get_messages(SECONDARY1_URL)
    assert second == ["msg0", "msg1", "msg2", "msg3"], f"GET: recieved {second}, wait ['msg0','msg1','msg2','msg3']"
