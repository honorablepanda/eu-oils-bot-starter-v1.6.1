# tools/ci/autodev.py
from __future__ import annotations
import os, sys, json, re, time, subprocess as sp, difflib, shutil
from pathlib import Path
from typing import List, Dict, Any, Optional

# ---------- Config ----------
REPO = Path(".").resolve()
LOG_DIR = REPO / "reports"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = LOG_DIR / "autodev_last_run.log"

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # must be set by you

# commands we allow the model to run (prefix checks)
SAFE_PREFIXES = [
    "python ", "python3 ", "py ",
    "pip ", "pip3 ",
    # allow PowerShell/python invocations used in this repo
    "pwsh ", "powershell ",
]

# timeouts (seconds)
CMD_TIMEOUT = 900  # 15 minutes max per command
RUN_TIMEOUT = int(os.environ.get("AUTODEV_MAX_SECONDS_PER_RUN", "2400"))  # cap per “run” step
MAX_ITERS = int(os.environ.get("AUTODEV_MAX_ITERS", "12"))

# ---------- OpenAI minimal client ----------
def _chat(messages: List[Dict[str, str]], response_format: Optional[str] = None) -> str:
    """
    Very small wrapper. Requires 'openai' package >=1.0:
        pip install openai
    """
    try:
        from openai import OpenAI
    except Exception:
        print("Please: pip install openai", file=sys.stderr)
        sys.exit(2)

    client = OpenAI(api_key=OPENAI_API_KEY)
    # Prefer JSON style, but don't hard fail if server doesn’t support forced JSON
    kwargs = {}
    if response_format == "json":
        kwargs["response_format"] = {"type": "json_object"}

    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.2,
        **kwargs,
    )
    return resp.choices[0].message.content or ""

# ---------- utils ----------
UNLOCK_RX = re.compile(r"\[UNLOCK\].*Press ENTER.*", re.I)
METRICS_RX = re.compile(r"^\[METRICS\]\s*(\{.*\})\s*$", re.M)
AUDIT_PASS_RX = re.compile(r"^\[AUDIT\]\s*PASS", re.M)
AUDIT_FAIL_RX = re.compile(r"^\[AUDIT\]\s*FAIL", re.M)

def log(msg: str):
    sys.stdout.write(msg + "\n")
    with RUN_LOG.open("a", encoding="utf-8") as fp:
        fp.write(msg + "\n")

def read_file(path: Path, limit: int = 200_000) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="ignore")
        if len(data) > limit:
            return data[:limit] + f"\n\n# [TRUNCATED {len(data)-limit} bytes]"
        return data
    except Exception as e:
        return f"[ERROR reading {path}: {e}]"

def write_file(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def apply_unified_diff(target: Path, diff_text: str) -> str:
    """
    Apply a unified diff to target file. Returns a summary string.
    """
    old = target.read_text(encoding="utf-8", errors="ignore") if target.exists() else ""
    try:
        patched = _apply_unified(old, diff_text)
        write_file(target, patched)
        return f"patched {target}"
    except Exception as e:
        return f"[PATCH FAILED {target}: {e}]"

def _apply_unified(original: str, diff_text: str) -> str:
    # crude unified diff applier using difflib: if fails, raise
    patched_lines = []
    try:
        # Parse diff hunks for the target file (ignore header paths)
        d = list(difflib.unified_diff(original.splitlines(True), [], lineterm=""))
        # We can't reconstruct from empty target with this; fallback to naive apply via difflib.restore
        # So instead, we try to use 'patch' style parse with python-patch if available.
        raise RuntimeError("Simple unified diff apply not supported; use full content write mode.")
    except Exception as e:
        raise e

def run_cmd(cmd: str, cwd: Path = REPO, wait_for_unlock: bool = True, timeout: Optional[int] = CMD_TIMEOUT) -> Dict[str, Any]:
    """
    Run a shell command safely and capture outputs.
    """
    if not any(cmd.startswith(p) for p in SAFE_PREFIXES):
        return {"cmd": cmd, "rc": 126, "stdout": "", "stderr": f"Blocked: not in SAFE_PREFIXES {SAFE_PREFIXES}"}

    with sp.Popen(
        cmd, cwd=str(cwd), shell=True,
        stdout=sp.PIPE, stderr=sp.STDOUT, stdin=sp.PIPE if wait_for_unlock else None,
        text=True, bufsize=1, universal_newlines=True
    ) as proc:
        out_lines = []
        start = time.time()
        try:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                sys.stdout.write(line)
                out_lines.append(line)
                if wait_for_unlock and UNLOCK_RX.search(line):
                    log("[AUTODEV] Unlock prompt detected → complete in browser, then press ENTER here.")
                    try:
                        input("[AUTODEV] Press ENTER to continue… ")
                        # forward newline to child if it asked via input()
                        try:
                            proc.stdin.write("\n"); proc.stdin.flush()
                        except Exception:
                            pass
                    except KeyboardInterrupt:
                        proc.kill()
                        return {"cmd": cmd, "rc": 130, "stdout": "".join(out_lines), "stderr": "Interrupted"}
                if timeout and (time.time() - start > timeout):
                    proc.kill()
                    return {"cmd": cmd, "rc": 124, "stdout": "".join(out_lines), "stderr": f"Timeout {timeout}s"}
        finally:
            try:
                proc.stdout.close()
            except Exception:
                pass
        rc = proc.wait()
        return {"cmd": cmd, "rc": rc, "stdout": "".join(out_lines), "stderr": ""}

def gather_health() -> Dict[str, Any]:
    """
    Run the quick health scans we already have and return their summaries.
    """
    health: Dict[str, Any] = {}

    # Phase-2/coverage auditor
    a = run_cmd(
        "python tools\\phase2\\phase2_audit.py --registry retailers\\registry.yaml --manifests manifests\\ --candidates-dir discovery\\ --check-website-id --check-canonical --check-gates",
        timeout=300
    )
    health["phase2_audit"] = {"rc": a["rc"], "output": a["stdout"][-8000:]}

    # Repo scan
    s = run_cmd("python tools\\health\\scan_repo.py --db .\\data\\eopt.sqlite --out-prefix autodev", timeout=180)
    health["scan_repo"] = {"rc": s["rc"], "output": s["stdout"][-6000:]}

    # Extract metrics if present
    m = METRICS_RX.findall(a["stdout"] + "\n" + s["stdout"])
    health["metrics_found"] = m[-1] if m else None
    health["audit_pass"] = bool(AUDIT_PASS_RX.search(a["stdout"])) and not bool(AUDIT_FAIL_RX.search(a["stdout"]))
    return health

SYSTEM_PROMPT = """You are an expert repo automation copilot for the 'EU Oils Price Bot' project.
Goal: Drive Phases 0, 1, 2 to PASS according to the Control Spec v2.

You operate by returning a STRICT JSON object describing the next step. The controller applies your file edits and runs your commands, then returns stdout/stderr + audit outputs.

Return ONLY JSON with this schema:
{
  "reason": "short explanation",
  "edits": [
    {"path": "relative/file/path.py", "mode": "write", "content": "<full file content>"},
    {"path": "relative/file/path.py", "mode": "patch", "unified_diff": "<unified diff>"}
  ],
  "commands": [
    "python -m eopt.cli run --run-id 2025-W42 --countries BE NL --mode real --targets ah_nl,jumbo_nl,carrefour_be,colruyt_be,vomar_nl",
    "python tools\\phase2\\phase2_audit.py --registry retailers\\registry.yaml --manifests manifests\\ --candidates-dir discovery\\ --check-website-id --check-canonical --check-gates"
  ],
  "done": false
}

Rules:
- Prefer minimal, safe diffs. If patching is hard, send full-file 'write' with complete content.
- Only use SAFE commands: python/pip/powershell/pwsh. No git/rm/curl/wget.
- Keep iterations small: make the smallest set of edits + commands to move one gate forward.
- Always finish with an audit or metrics command to measure progress.
- Stop (done=true) ONLY when Phase-2 audit passes and identifier rate ≥ 0.60 and canonical weekly/master exist and 'website_id' is present in store_context.
- Never output prose outside JSON.
"""

def main():
    if not OPENAI_API_KEY:
        print("Set OPENAI_API_KEY in your environment.", file=sys.stderr)
        sys.exit(2)

    # Seed conversation with repository snapshot
    file_manifest = []
    for p in REPO.glob("**/*"):
        if p.is_file() and len(p.parts) < 8 and p.stat().st_size < 200_000:
            # sample only important dirs
            if any(str(p).startswith(str(REPO / d)) for d in [
                "tools/phase1", "tools/phase2", "tools/discovery", "tools/health",
                "src/eopt", "configs", "manifests", "retailers"
            ]):
                file_manifest.append(str(p.relative_to(REPO)))

    msgs = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps({
            "repo_root": str(REPO),
            "os": os.name,
            "manifest_sample": file_manifest[:400],
            "note": "You can ask for specific files by including a read request as an 'edit' with mode='read'."
        })}
    ]

    for it in range(1, MAX_ITERS + 1):
        print(f"\n==== AUTODEV ITERATION {it}/{MAX_ITERS} ====\n")
        resp = _chat(msgs, response_format="json")
        # Try parse JSON (strip code fences if any)
        mx = re.search(r"\{.*\}\s*$", resp, re.S)
        if mx:
            payload_txt = mx.group(0)
        else:
            payload_txt = resp.strip()

        try:
            plan = json.loads(payload_txt)
        except Exception as e:
            log(f"[AUTODEV] Bad JSON from model: {e}\n{resp[:5000]}")
            break

        # handle "read" edit requests to fetch file contents into next round
        readbacks: Dict[str, str] = {}
        edits: List[Dict[str, Any]] = plan.get("edits") or []
        materialized_edits = []
        for ed in edits:
            mode = ed.get("mode")
            path = ed.get("path")
            if not path:
                continue
            fpath = REPO / path
            if mode == "read":
                readbacks[path] = read_file(fpath)
                continue
            materialized_edits.append(ed)

        # If there were reads, push contents and ask again immediately
        if readbacks:
            msgs.append({"role": "assistant", "content": json.dumps(plan)})
            msgs.append({"role": "user", "content": json.dumps({"file_contents": readbacks})})
            continue

        # apply writes/patches
        applied = []
        for ed in materialized_edits:
            path = ed["path"]; mode = ed.get("mode"); fpath = REPO / path
            if mode == "write":
                write_file(fpath, ed.get("content") or "")
                applied.append(f"[write] {path} ({len(ed.get('content') or '')} bytes)")
            elif mode == "patch":
                summary = apply_unified_diff(fpath, ed.get("unified_diff") or "")
                applied.append(summary)
            else:
                applied.append(f"[skip] {path}: unknown mode={mode}")

        # run commands
        results = []
        for cmd in plan.get("commands") or []:
            res = run_cmd(cmd, timeout=RUN_TIMEOUT)
            results.append({"cmd": cmd, "rc": res["rc"], "stdout_tail": res["stdout"][-12000:]})

        # refresh health after each iteration
        health = gather_health()

        # build next message
        msgs.append({"role": "assistant", "content": json.dumps({
            "applied_edits": applied,
            "command_results": results,
            "health": health
        })})

        if plan.get("done"):
            ok = health.get("audit_pass", False)
            # also check identifier rate if present
            ident_rate = 0.0
            if health.get("metrics_found"):
                try:
                    ident_rate = float(json.loads(health["metrics_found"]).get("identifier_rate_overall") or 0.0)
                except Exception:
                    pass
            if ok and ident_rate >= 0.60:
                log("[AUTODEV] ✅ Done per model and audits.")
                return
            else:
                log("[AUTODEV] Model returned done=true, but gates not green — continuing.")

        # Ask model what next, including tails
        msgs.append({"role": "user", "content": json.dumps({
            "next_prompt": "Provide the next minimal set of edits and commands.",
            "applied_edits": applied,
            "command_results": results,
            "phase2_audit_tail": health["phase2_audit"]["output"],
            "scan_repo_tail": health["scan_repo"]["output"]
        })})

    log(f"[AUTODEV] ❌ Reached max iterations ({MAX_ITERS}). See {RUN_LOG}")

if __name__ == "__main__":
    main()
