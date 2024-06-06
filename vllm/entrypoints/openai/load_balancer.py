import argparse
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request, Response

@dataclass
class Worker:
    url: str
    device_kv_cache_utilization: float


parser = argparse.ArgumentParser()
parser.add_argument('--num-workers', '-w', type=int, default=8)
parser.add_argument('--max-connections', '-mc', type=int, default=1024)
parser.add_argument('--max-keepalive-connections', '-mk', type=int, default=256)
args = parser.parse_args()

# TODO: adjust timeout and limits
timeout = 300.0
limits = httpx.Limits(max_keepalive_connections=args.max_keepalive_connections,
                      max_connections=args.max_connections)
workers = [
    Worker(f"http://localhost:808{i}", 0.0)
    for i in range(1, args.num_workers + 1)
]
app = FastAPI()


@app.get("/v1/models")
async def show_available_models():
    url = f"{workers[0].url}/v1/models"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()


@app.post("/v1/completions")
async def load_balancer(raw_request: Request):
    body = await raw_request.body()
    headers = dict(raw_request.headers)

    # update worker KV-cache utilization
    # TODO: consider reducing frequency of updates
    async with httpx.AsyncClient() as client:
        for worker in workers:
            response = await client.get(worker.url +
                                        "/device/kv_cache_utilization")
            worker.device_kv_cache_utilization = response.json()["utilization"]

    # select worker with the lowest KV-cache utilization
    best_fit = min(workers,
                   key=lambda worker: worker.device_kv_cache_utilization)
    url = f'{best_fit.url}/v1/completions'

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        response = await client.post(url, content=body, headers=headers)
    return Response(content=response.content,
                    status_code=response.status_code,
                    headers=dict(response.headers))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8080, timeout_keep_alive=5)
