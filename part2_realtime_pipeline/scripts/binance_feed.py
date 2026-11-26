import asyncio, json, os, sys, time, websockets

URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
IDLE_TIMEOUT_SECONDS = 30
MODE = sys.argv[1] if len(sys.argv) > 1 else "files" 
OUT = "./binance_out"
clients = set()

def write_file_batch(lines):
    if not lines:
        return
    tmp = f"{OUT}/.tmp-{time.time_ns()}"
    with open(tmp, "w") as f:
        f.writelines(lines)
    os.rename(tmp, f"{OUT}/part-{time.time_ns()}.json")

async def tcp_handler(reader, writer):
    clients.add(writer)
    try:
        await reader.read()
    finally:
        clients.discard(writer)
        writer.close()

async def broadcast(line):
    for writer in list(clients):
        try:
            writer.write(line)
            await writer.drain()
        except (ConnectionError, BrokenPipeError):
            clients.discard(writer)
            writer.close()

async def main():
    buf, last = [], time.time()
    if MODE == "tcp":
        server = await asyncio.start_server(tcp_handler, "0.0.0.0", 9999)
        asyncio.create_task(server.serve_forever())
    else:
        if MODE != "files":
            raise ValueError("MODE must be 'files' or 'tcp'")
        os.makedirs(OUT, exist_ok=True)
    async for ws in websockets.connect(URL, ping_interval=20):
        print("Binance WebSocket connected.", flush=True)
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    print(
                        f"No Binance messages for {IDLE_TIMEOUT_SECONDS}s; reconnecting.",
                        flush=True,
                    )
                    if MODE == "files":
                        write_file_batch(buf)
                        buf, last = [], time.time()
                    await ws.close(code=1011, reason="idle timeout")
                    break
                line = json.dumps(json.loads(msg), separators=(",", ":")) + "\n"
                if MODE == "tcp":
                    await broadcast(line.encode())
                else:
                    buf.append(line)
                    if time.time() - last >= 2:
                        write_file_batch(buf)
                        buf, last = [], time.time()
        except websockets.ConnectionClosed:
            print("Binance WebSocket closed; reconnecting.", flush=True)
            continue

asyncio.run(main())
