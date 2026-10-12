"""
Ray Serve app: OpenAI-compatible LLM serving on AWS Trainium (Neuron).

Architecture (and why):
  Ray Serve is the router + autoscaler. Each Serve replica owns exactly ONE
  NeuronCore and runs its own vLLM engine behind an OpenAI-compatible HTTP
  surface.

  Ray natively understands NeuronCores: a replica that requests
  `resources={"neuron_cores": 1}` gets `NEURON_RT_VISIBLE_CORES` set by Ray to
  its assigned core index. That is what makes bin-packing work -- a
  trn1.2xlarge has 1 Trainium chip = 2 NeuronCores, so TWO replicas share one
  chip instead of stranding the whole accelerator on a single model.

  The replica launches vLLM via its own `vllm serve` entrypoint on a private
  loopback port and proxies to it. We deliberately do NOT re-implement the
  OpenAI chat surface: `vllm serve` already provides chat templating,
  streaming, and usage accounting, and it is the exact path AWS ships and
  supports in the Neuron DLC. Ray Serve keeps the routing and autoscaling.
"""

import fcntl
import hashlib
import importlib.metadata
import logging
import os
import shlex
import shutil
import socket
import subprocess
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ray import serve

logger = logging.getLogger("serve_app")

MODEL_ID = os.environ.get("MODEL_ID", "Qwen/Qwen2.5-1.5B-Instruct")
# Neuron compiles the model graph on first load; that can take many minutes.
ENGINE_STARTUP_TIMEOUT_S = int(os.environ.get("ENGINE_STARTUP_TIMEOUT_S", "3600"))
MAX_MODEL_LEN = os.environ.get("MAX_MODEL_LEN", "2048")
MAX_NUM_SEQS = os.environ.get("MAX_NUM_SEQS", "8")
# Two engine flags this image needs (vLLM 0.16 + vLLM-Neuron 0.5.3), both
# verified on trn1.2xlarge:
#
#   --block-size 32
#       vLLM 0.16 enables prefix caching by default, and vLLM-Neuron then
#       requires an explicit block size (blockwise KV-cache layout). Do NOT
#       switch prefix caching off instead: that selects the
#       one-block-per-sequence layout, which returns a correct first token and
#       then garbage.
#
#   --no-async-scheduling
#       vLLM 0.16 enables async scheduling by default, which keeps two batches
#       in flight. The Neuron model runner caches only one batch of logits, so
#       under concurrent load the engine crashes with "sample_tokens() called
#       without prior execute_model()" and every in-flight request fails.
#
#   --num-gpu-blocks-override 512
#       The compiled Neuron graph holds exactly
#       max_num_seqs * max_model_len / block_size = 8 * 2048 / 32 = 512 KV
#       blocks, but vLLM sizes the scheduler's KV pool from the runtime's free
#       device memory (~26,700 blocks here). Under concurrent load the
#       scheduler hands out block IDs past 511 and the Neuron runtime fails
#       with "status=1006 Execution Out-Of-Bounds Memory Access". The override
#       pins both sides to the same count (vLLM-Neuron adds +1 for the null
#       block itself). Recompute it if you change MAX_NUM_SEQS, MAX_MODEL_LEN
#       or --block-size.
VLLM_EXTRA_ARGS = os.environ.get(
    "VLLM_EXTRA_ARGS",
    "--block-size 32 --no-async-scheduling --num-gpu-blocks-override 512",
)

# Where compiled Neuron model artifacts are kept, on the node's host cache.
#
# Why this exists: when vLLM-Neuron is NOT given NEURON_COMPILED_ARTIFACTS, it
# derives a directory inside the model snapshot and then deletes it
# (mkdir + rmtree in neuronx_distributed_model_loader._get_compiled_model_path)
# before trying to load from it. Every replica therefore rebuilt the model
# (~85 s on trn1.2xlarge) even though the previous replica had just compiled
# it. Pointing NEURON_COMPILED_ARTIFACTS at a stable, config-keyed directory
# lets the second and later replicas on a node skip the build entirely.
ARTIFACT_ROOT = os.environ.get("NEURON_ARTIFACT_ROOT", "/cache/neuron-artifacts")
READY_MARKER = ".ready"

api = FastAPI()


def _artifact_dir(model_path: str) -> str:
    """One directory per (model, engine config, vLLM-Neuron version), so a flag
    change never loads artifacts compiled for a different configuration."""
    try:
        plugin_version = importlib.metadata.version("vllm-neuron")
    except importlib.metadata.PackageNotFoundError:
        plugin_version = "unknown"
    key = "|".join(
        [MODEL_ID, model_path, MAX_MODEL_LEN, MAX_NUM_SEQS, VLLM_EXTRA_ARGS, "tp1", plugin_version]
    )
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return os.path.join(ARTIFACT_ROOT, f"{MODEL_ID.replace('/', '--')}-{digest}")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _resolve_model_path() -> str:
    """Prefer the on-node cache so replicas don't re-download the weights."""
    try:
        from huggingface_hub import snapshot_download

        return snapshot_download(MODEL_ID)
    except Exception as exc:  # fall back to letting vLLM resolve the id
        logger.warning("snapshot_download failed (%s); passing model id to vLLM", exc)
        return MODEL_ID


@serve.deployment(
    name="VLLMNeuron",
    # Replica autoscaling: replicas scale on in-flight requests. Each replica
    # needs a free NeuronCore, so the accelerator pool is the real ceiling.
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 1,
        "max_replicas": 4,
        "target_ongoing_requests": 4,
        "upscale_delay_s": 10,
        "downscale_delay_s": 120,
    },
    max_ongoing_requests=8,
    # One NeuronCore per replica -> two replicas bin-pack onto one Trainium chip.
    ray_actor_options={"num_cpus": 1, "resources": {"neuron_cores": 1}},
)
@serve.ingress(api)
class VLLMNeuron:
    def __init__(self) -> None:
        self._port = _free_port()
        self._base = f"http://127.0.0.1:{self._port}"
        self._client = httpx.AsyncClient(base_url=self._base, timeout=httpx.Timeout(600.0))
        cores = os.environ.get("NEURON_RT_VISIBLE_CORES", "<unset>")
        model_path = _resolve_model_path()
        artifacts = _artifact_dir(model_path)
        os.makedirs(ARTIFACT_ROOT, exist_ok=True)

        # Only one replica per node may compile a given configuration; the
        # others wait here, then load what it produced. The lock file lives on
        # the shared host cache, so it covers every replica in the pod.
        lock_fh = None
        if not os.path.exists(os.path.join(artifacts, READY_MARKER)):
            lock_fh = open(artifacts + ".lock", "w")
            logger.info("waiting for compile lock %s", artifacts + ".lock")
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
            if os.path.exists(os.path.join(artifacts, READY_MARKER)):
                # Another replica finished compiling while we waited.
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
                lock_fh.close()
                lock_fh = None
            else:
                # Anything here without the marker is a partial compile.
                shutil.rmtree(artifacts, ignore_errors=True)

        mode = "compiling" if lock_fh else "loading precompiled artifacts"
        logger.info(
            "starting vLLM on NeuronCore(s) %s, port %s (%s: %s)",
            cores, self._port, mode, artifacts,
        )

        # Keep vLLM's own output so a startup failure reports the real cause
        # instead of just a non-zero exit code.
        self._log_path = f"/tmp/vllm-replica-{self._port}.log"
        self._log = open(self._log_path, "w+")

        env = os.environ.copy()
        env["NEURON_COMPILED_ARTIFACTS"] = artifacts

        self._proc = subprocess.Popen(
            [
                "vllm",
                "serve",
                model_path,
                "--served-model-name",
                MODEL_ID,
                "--host",
                "127.0.0.1",
                "--port",
                str(self._port),
                # One core per replica, so no tensor parallelism inside a replica.
                "--tensor-parallel-size",
                "1",
                "--max-num-seqs",
                str(MAX_NUM_SEQS),
                "--max-model-len",
                str(MAX_MODEL_LEN),
                # Engine flags come from the RayService (VLLM_EXTRA_ARGS) so they
                # can be tuned without rebuilding anything. See the note there.
                *shlex.split(VLLM_EXTRA_ARGS),
            ],
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        try:
            self._await_engine()
            if lock_fh is not None:
                # The engine is serving, so the artifacts are complete.
                open(os.path.join(artifacts, READY_MARKER), "w").close()
        finally:
            if lock_fh is not None:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
                lock_fh.close()

    def _log_tail(self, lines: int = 15) -> str:
        try:
            with open(self._log_path) as fh:
                return "".join(fh.readlines()[-lines:])
        except OSError:
            return "<no vLLM output captured>"

    def _await_engine(self) -> None:
        deadline = time.time() + ENGINE_STARTUP_TIMEOUT_S
        with httpx.Client(base_url=self._base, timeout=5.0) as probe:
            while time.time() < deadline:
                if self._proc.poll() is not None:
                    raise RuntimeError(
                        f"vLLM exited during startup with code {self._proc.returncode}.\n"
                        f"Last output:\n{self._log_tail()}"
                    )
                try:
                    if probe.get("/health").status_code == 200:
                        logger.info("vLLM engine ready on %s", self._base)
                        return
                except httpx.HTTPError:
                    pass
                time.sleep(5)
        raise RuntimeError(
            f"vLLM engine not ready within {ENGINE_STARTUP_TIMEOUT_S}s.\n"
            f"Last output:\n{self._log_tail()}"
        )

    def check_health(self) -> None:
        """Called by Ray Serve every few seconds. Raising marks the replica
        unhealthy, so Serve replaces it instead of routing traffic to a dead
        vLLM process."""
        if self._proc.poll() is not None:
            raise RuntimeError(
                f"vLLM exited with code {self._proc.returncode}.\n"
                f"Last output:\n{self._log_tail()}"
            )

    # --- OpenAI-compatible surface -----------------------------------------
    async def _proxy(self, request: Request, path: str):
        body = await request.body()
        headers = {"content-type": request.headers.get("content-type", "application/json")}
        upstream = self._client.build_request("POST", path, content=body, headers=headers)
        response = await self._client.send(upstream, stream=True)

        # Pass streaming responses (SSE) straight through.
        if response.headers.get("content-type", "").startswith("text/event-stream"):
            async def body_iter():
                try:
                    async for chunk in response.aiter_raw():
                        yield chunk
                finally:
                    await response.aclose()

            return StreamingResponse(
                body_iter(),
                status_code=response.status_code,
                media_type="text/event-stream",
            )

        await response.aread()
        await response.aclose()
        return JSONResponse(status_code=response.status_code, content=response.json())

    @api.post("/v1/chat/completions")
    async def chat_completions(self, request: Request):
        return await self._proxy(request, "/v1/chat/completions")

    @api.post("/v1/completions")
    async def completions(self, request: Request):
        return await self._proxy(request, "/v1/completions")

    @api.get("/v1/models")
    async def models(self):
        response = await self._client.get("/v1/models")
        return JSONResponse(status_code=response.status_code, content=response.json())

    @api.get("/-/health")
    async def health(self):
        return {
            "status": "ok",
            "model": MODEL_ID,
            "neuron_cores": os.environ.get("NEURON_RT_VISIBLE_CORES", "<unset>"),
            "node_ip": os.environ.get("MY_POD_IP", ""),
        }

    def __del__(self) -> None:
        proc = getattr(self, "_proc", None)
        if proc is not None and proc.poll() is None:
            proc.terminate()


app = VLLMNeuron.bind()
