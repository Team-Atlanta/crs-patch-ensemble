"""
crs-patch-ensemble patcher module.

Monitors exchange directory for patches from other CRSs,
validates each patch (build + POV + test), and uses Claude Code
to select the most semantically correct fix when multiple candidates pass.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from libCRS.base import DataType
from libCRS.cli.main import init_crs_utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ensemble")

# --- Configuration ---
HARNESS = os.environ.get("OSS_CRS_TARGET_HARNESS", "")
BUILDER_MODULE = os.environ.get("BUILDER_MODULE", "inc-builder-asan")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
SUBMISSION_FLUSH_WAIT_SECS = int(os.environ.get("SUBMISSION_FLUSH_WAIT_SECS", "12"))
SUBMIT_MAX_RETRIES = 3
SUBMIT_RETRY_DELAY = 2

# Selector configuration
SELECTOR_MODEL = os.environ.get("SELECTOR_MODEL", "")
try:
    SELECTOR_TIMEOUT = int(os.environ.get("SELECTOR_TIMEOUT", "0"))
except ValueError:
    SELECTOR_TIMEOUT = 0

# Directories
WORK_DIR = Path("/work")
PATCHES_DIR = Path("/patches")
CANDIDATE_DIR = WORK_DIR / "candidates"
POV_DIR = WORK_DIR / "povs"
SELECTOR_DIR = WORK_DIR / "selector"
SOURCE_DIR = Path("/OSS_CRS_BUILD_OUT_DIR/src")
STATE_FILE = WORK_DIR / "ensemble_state.json"

# --- Selector Prompt Templates ---

LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

SELECTION_PROMPT_HEADER = """\
I have {num_patches} candidate patch(es) that successfully fix the following bug:

Crash log(s): {crash_log_files}

The full source code is available at `{source_dir}` for reference.

Here are the candidate patches:

"""

SELECTION_PROMPT_PATCH = """\
=== PATCH {label} ===
```diff
{diff}
```

"""

SELECTION_PROMPT_FOOTER = """\
## Instructions

Your task is to select the patch that is most semantically correct:
- All patches passed automated testing (compilation, POV execution, test suite), \
but may only fix the symptom rather than the root cause.
- Select the one that indeed fixes the bug and does NOT change the original \
functionality of the code.
- Semantically equivalent changes are OK as long as they exhibit same behavior \
as original code in the non-vulnerable case.
- Consider code quality and whether a maintainer would accept the change upstream.

Write your selection to a file named `selection.json` with the following JSON format:
{{"selection": "<label>", "reason": "<brief explanation>"}}

Valid values for "selection":
{valid_options}
You MUST select one of the above options. Do not modify any source files.
"""


# --- Data Model ---


@dataclass
class Patch:
    path: Path
    build_ok: bool = False
    build_id: str | None = None
    pov_results: dict[str, bool] = field(default_factory=dict)
    pov_pass_count: int = 0
    pov_total: int = 0
    test_ok: bool = False
    error: str | None = None


# --- Helpers ---


def _parse_selection(raw: str) -> str:
    """Extract the 'selection' field from JSON, or return raw as fallback."""
    try:
        data = json.loads(raw)
        return data.get("selection", "").strip()
    except (json.JSONDecodeError, AttributeError):
        return raw.strip()


def _submit_with_retry(crs, data_type: DataType, path: Path) -> bool:
    """Submit a file via libCRS with retry on failure."""
    for attempt in range(1, SUBMIT_MAX_RETRIES + 1):
        try:
            crs.submit(data_type, path)
            return True
        except Exception:
            logger.exception(
                "Submit failed (attempt %d/%d): %s",
                attempt, SUBMIT_MAX_RETRIES, path.name,
            )
            if attempt < SUBMIT_MAX_RETRIES:
                time.sleep(SUBMIT_RETRY_DELAY)
    logger.error("Submit exhausted all retries: %s", path.name)
    return False


# --- Selector Setup ---


def setup_selector(work_dir: Path) -> None:
    """Configure Claude Code CLI for the selector."""
    llm_api_url = os.environ.get("OSS_CRS_LLM_API_URL", "")
    llm_api_key = os.environ.get("OSS_CRS_LLM_API_KEY", "")

    os.environ["IS_SANDBOX"] = "1"

    if llm_api_url and llm_api_key:
        os.environ["ANTHROPIC_BASE_URL"] = llm_api_url
        os.environ["ANTHROPIC_AUTH_TOKEN"] = llm_api_key
        os.environ["ANTHROPIC_API_KEY"] = ""
        logger.info("Selector configured with LiteLLM proxy: %s", llm_api_url)
    else:
        logger.warning("No LLM API URL/key set, selector may not work")

    model = SELECTOR_MODEL or os.environ.get("ANTHROPIC_MODEL", "")
    logger.info("Selector model: %s", model or "(default)")

    claude_config = {
        "numStartups": 0,
        "autoUpdaterStatus": "disabled",
        "userID": "-",
        "hasCompletedOnboarding": True,
        "lastOnboardingVersion": "1.0.0",
        "projects": {
            str(work_dir): {
                "hasTrustDialogAccepted": True,
                "hasCompletedProjectOnboarding": True,
            }
        },
    }
    claude_json = Path.home() / ".claude.json"
    claude_json.write_text(json.dumps(claude_config))
    claude_json.chmod(0o600)
    logger.info("Wrote Claude config to %s", claude_json)

    global_gitignore = Path.home() / ".gitignore"
    global_gitignore.write_text("selection.json\n")
    subprocess.run(
        ["git", "config", "--global", "core.excludesFile", str(global_gitignore)],
        capture_output=True,
    )


# --- Baseline ---


def reproduce_crashes(crs, pov_files: list[Path]) -> list[Path]:
    """Run each POV against the base build to collect crash logs.

    Returns crash log file paths. Raises RuntimeError if ALL POVs fail.
    """
    crash_log_files = []
    for i, pov_path in enumerate(pov_files):
        response_dir = WORK_DIR / f"base_crash_{i}"
        response_dir.mkdir(parents=True, exist_ok=True)
        try:
            exit_code = crs.run_pov(
                pov_path, HARNESS, "base", response_dir, BUILDER_MODULE
            )
        except Exception:
            logger.exception("Failed to run base POV %d (%s)", i, pov_path.name)
            continue
        stderr_path = response_dir / "pov_stderr.log"
        crash_log_files.append(stderr_path)
        log_size = stderr_path.stat().st_size if stderr_path.exists() else 0
        logger.info(
            "Base POV %d (%s): exit=%d log_size=%d",
            i, pov_path.name, exit_code, log_size,
        )
    if not crash_log_files:
        raise RuntimeError("All base POV runs failed, no crash logs collected")
    return crash_log_files


def run_base_test(crs) -> bool:
    """Run test suite against the base build.

    Returns True if base test passes. When False, patched builds are
    not required to pass the test either (test was already broken).
    """
    response_dir = WORK_DIR / "base_test"
    response_dir.mkdir(parents=True, exist_ok=True)
    try:
        exit_code = crs.run_test("base", response_dir, BUILDER_MODULE)
    except Exception:
        logger.exception("Failed to run base test, treating as failed")
        return False
    ok = exit_code == 0
    logger.info("Base test: exit=%d ok=%s", exit_code, ok)
    return ok


# --- Ensemble Manager ---


class EnsembleManager:
    def __init__(
        self, crs, pov_files: list[Path],
        crash_log_files: list[Path], base_test_ok: bool,
    ):
        self.crs = crs
        self.pov_files = pov_files
        self.crash_log_files = crash_log_files
        self.base_test_ok = base_test_ok
        self.patches: dict[str, Patch] = {}
        self.best: Patch | None = None
        self._state: dict = {
            "baseline": {
                "builder_ok": True,
                "base_test_ok": base_test_ok,
                "pov_results": [
                    {"pov": p.name, "crash_log": str(l), "crash_log_exists": l.exists()}
                    for p, l in zip(pov_files, crash_log_files)
                ],
            },
            "patches": {},
            "ensemble_selection": None,
        }

    # --- Core flow ---

    def handle_new_patches(self, new_files: list[str]) -> None:
        """Validate each new patch (build + POV + test)."""
        for fname in new_files:
            if fname in self.patches:
                continue
            logger.info("New candidate: %s", fname)
            patch = Patch(path=CANDIDATE_DIR / fname, pov_total=len(self.pov_files))
            self.patches[fname] = patch
            self._validate(patch)
            self._record_patch(fname, patch)

    def select_and_submit(self) -> None:
        """Select best patch from all validated candidates and submit. Called once at the end."""
        validated = self.get_validated()
        if not validated:
            logger.info("No validated patches to select from")
            return

        logger.info("Selecting from %d validated patch(es)", len(validated))

        if len(validated) == 1:
            selected = validated[0]
            logger.info("Single validated patch, auto-selecting: %s", selected.path.name)
            self._record_selection(validated, selected, "auto")
        else:
            selected = self._select_with_claude(validated)
            if selected:
                logger.info("Selector chose: %s (from %d candidates)", selected.path.name, len(validated))
                self._record_selection(validated, selected, "claude-code")
            else:
                selected = validated[0]
                logger.warning("Selector failed, falling back to: %s", selected.path.name)
                self._record_selection(validated, selected, "fallback")

        self.best = selected
        logger.info("Submitting selected patch: %s", self.best.path.name)
        _submit_with_retry(self.crs, DataType.PATCH, self.best.path)

    # --- Validation ---

    def _validate(self, patch: Patch) -> None:
        """Build + POV + test. Exceptions skip this patch."""
        try:
            # Build
            build_dir = WORK_DIR / "validate" / patch.path.stem / "build"
            build_dir.mkdir(parents=True, exist_ok=True)
            build_exit = self.crs.apply_patch_build(
                patch.path, build_dir, BUILDER_MODULE
            )
            if build_exit != 0:
                logger.info("Patch %s: build failed (exit=%d)", patch.path.name, build_exit)
                return

            patch.build_ok = True
            build_id_file = build_dir / "build_id"
            if not build_id_file.exists():
                logger.warning("Patch %s: build ok but no build_id", patch.path.name)
                return
            patch.build_id = build_id_file.read_text().strip()

            # POVs — early-return on first failure
            for pov_path in self.pov_files:
                pov_dir = WORK_DIR / "validate" / patch.path.stem / f"pov-{pov_path.stem}"
                pov_dir.mkdir(parents=True, exist_ok=True)
                pov_exit = self.crs.run_pov(
                    pov_path, HARNESS, patch.build_id, pov_dir, BUILDER_MODULE
                )
                passed = pov_exit == 0
                patch.pov_results[pov_path.name] = passed
                if passed:
                    patch.pov_pass_count += 1
                else:
                    logger.info("Patch %s: POV %s failed", patch.path.name, pov_path.name)
                    return

            # Test
            test_dir = WORK_DIR / "validate" / patch.path.stem / "test"
            test_dir.mkdir(parents=True, exist_ok=True)
            test_exit = self.crs.run_test(patch.build_id, test_dir, BUILDER_MODULE)
            patch.test_ok = test_exit == 0

            logger.info(
                "Patch %s: build=%s pov=%d/%d test=%s",
                patch.path.name, patch.build_ok,
                patch.pov_pass_count, patch.pov_total, patch.test_ok,
            )
        except Exception as e:
            logger.exception("Validation failed for %s", patch.path.name)
            patch.error = str(e)

    def _is_fully_validated(self, p: Patch) -> bool:
        if not p.build_ok or p.pov_pass_count != p.pov_total or p.pov_total == 0:
            return False
        if self.base_test_ok and not p.test_ok:
            return False
        return True

    def get_validated(self) -> list[Patch]:
        return [p for p in self.patches.values() if self._is_fully_validated(p)]

    # --- Selection ---

    def _select_with_claude(self, validated: list[Patch]) -> Patch | None:
        """Use Claude Code to pick the best patch from 2+ candidates."""
        logger.info(
            "Running selector on %d patches: %s",
            len(validated), [p.path.name for p in validated],
        )

        # Label mapping (A, B, C, ... to avoid bias)
        label_to_patch: dict[str, Patch] = {}
        for i, patch in enumerate(validated):
            label_to_patch[LABELS[i]] = patch
            logger.info("Label %s -> %s", LABELS[i], patch.path.name)

        # Build prompt
        crash_log_refs = ", ".join(
            f"`{p}`" for p in self.crash_log_files if p.exists()
        )
        prompt = SELECTION_PROMPT_HEADER.format(
            num_patches=len(validated),
            crash_log_files=crash_log_refs or "(no crash logs available)",
            source_dir=SOURCE_DIR,
        )
        for label, patch in label_to_patch.items():
            diff = patch.path.read_text(errors="replace")
            prompt += SELECTION_PROMPT_PATCH.format(label=label, diff=diff)

        valid_options = "\n".join(f"- `{label}`" for label in label_to_patch)
        prompt += SELECTION_PROMPT_FOOTER.format(valid_options=valid_options)

        return self._run_selector(prompt, label_to_patch)

    def _run_selector(
        self, prompt: str, label_to_patch: dict[str, Patch],
    ) -> Patch | None:
        """Invoke Claude Code CLI and parse its selection."""
        run_dir = SELECTOR_DIR / f"run_{int(time.time())}"
        run_dir.mkdir(parents=True, exist_ok=True)

        (run_dir / "prompt.txt").write_text(prompt)
        selection_file = run_dir / "selection.json"

        cmd = [
            "claude", "-p",
            "-d", str(run_dir),
            "--dangerously-skip-permissions",
        ]
        model = SELECTOR_MODEL or os.environ.get("ANTHROPIC_MODEL", "")
        if model:
            cmd.extend(["--model", model])

        stdout_log = run_dir / "claude_stdout.log"
        stderr_log = run_dir / "claude_stderr.log"
        debug_log = run_dir / "claude_debug.log"
        cmd.extend(["--debug-file", str(debug_log)])

        logger.info("Starting selector: %s", " ".join(cmd))

        try:
            with open(stdout_log, "w") as out_f, open(stderr_log, "w") as err_f:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    cwd=run_dir,
                    start_new_session=True,
                )
                proc.stdin.write(prompt)
                proc.stdin.close()
                proc.wait(timeout=SELECTOR_TIMEOUT or None)
                logger.info("Selector exit code: %d", proc.returncode)
        except subprocess.TimeoutExpired:
            logger.warning("Selector timed out (%ds), killing", SELECTOR_TIMEOUT)
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                time.sleep(2)
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
        except Exception:
            logger.exception("Error running selector")
            return None

        # Make Claude chat history readable for post-run analysis
        claude_dir = Path.home() / ".claude"
        if claude_dir.exists():
            subprocess.run(
                ["chmod", "-R", "og+rX", str(claude_dir)],
                capture_output=True,
            )

        if not selection_file.exists():
            logger.warning("Selector did not produce selection.json")
            logger.info("Check logs at: %s", run_dir)
            return None

        raw = selection_file.read_text().strip()
        logger.info("Selector raw output: %s", raw)
        (run_dir / "selection_result.json").write_text(raw)

        selection = _parse_selection(raw)
        if selection in label_to_patch:
            patch = label_to_patch[selection]
            logger.info("Selector chose: label=%s patch=%s", selection, patch.path.name)
            return patch

        logger.warning("Selector returned invalid label: '%s'", selection)
        return None

    # --- State tracking ---

    def _record_patch(self, fname: str, patch: Patch) -> None:
        self._state["patches"][fname] = {
            "build_ok": patch.build_ok,
            "build_id": patch.build_id,
            "pov_results": patch.pov_results,
            "pov_pass": f"{patch.pov_pass_count}/{patch.pov_total}",
            "test_ok": patch.test_ok,
            "fully_validated": self._is_fully_validated(patch),
            "error": patch.error,
        }
        self.dump_state()

    def _record_selection(
        self, candidates: list[Patch], selected: Patch, method: str,
    ) -> None:
        self._state["ensemble_selection"] = {
            "candidates": [p.path.name for p in candidates],
            "selected": selected.path.name,
            "method": method,
        }
        self.dump_state()

    def dump_state(self) -> None:
        try:
            STATE_FILE.write_text(json.dumps(self._state, indent=2) + "\n")
        except Exception:
            logger.exception("Failed to write state file")


# --- Entry point functions ---


def wait_for_builder(crs) -> bool:
    try:
        domain = crs.get_service_domain(BUILDER_MODULE)
        logger.info("Builder sidecar '%s' resolved to %s", BUILDER_MODULE, domain)
        return True
    except RuntimeError as e:
        logger.error("Builder domain resolution failed: %s", e)
        return False


def init_libcrs():
    """Initialize libCRS and register submission directory."""
    crs = init_crs_utils()
    PATCHES_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(
        target=crs.register_submit_dir,
        args=(DataType.PATCH, PATCHES_DIR),
        daemon=True,
    ).start()
    return crs


def fetch_povs(crs) -> list[Path]:
    """Fetch POV files from exchange. Exits if none found."""
    crs.fetch(DataType.POV, POV_DIR)
    pov_files = sorted(
        f for f in POV_DIR.rglob("*")
        if f.is_file() and not f.name.startswith(".")
    )
    logger.info("Fetched %d POV(s)", len(pov_files))
    if not pov_files:
        logger.error("No POV files found")
        sys.exit(1)
    return pov_files


def run_baseline(crs, pov_files: list[Path]) -> tuple[list[Path], bool]:
    """Reproduce crashes and run base test. Exits on fatal failures."""
    if not wait_for_builder(crs):
        sys.exit(1)
    try:
        crash_log_files = reproduce_crashes(crs, pov_files)
    except RuntimeError as e:
        logger.error("Crash reproduction failed, exiting: %s", e)
        sys.exit(1)
    base_test_ok = run_base_test(crs)
    return crash_log_files, base_test_ok


def setup_shared_dirs(crs) -> None:
    """Register shared dirs and configure Claude Code selector."""
    claude_log_dir = Path.home() / ".claude"
    crs.register_shared_dir(claude_log_dir, "claude-logs")
    crs.register_shared_dir(SELECTOR_DIR, "selector")
    SELECTOR_DIR.mkdir(parents=True, exist_ok=True)
    setup_selector(SELECTOR_DIR)


def _handle_ready_signal(crs, manager: EnsembleManager) -> None:
    """All patch CRSes exited: wait for exchange flush, final fetch, final ensemble."""
    logger.info("Lifecycle ready signal received, waiting %ds for exchange flush",
                SUBMISSION_FLUSH_WAIT_SECS)
    time.sleep(SUBMISSION_FLUSH_WAIT_SECS)

    final_files = crs.fetch(DataType.PATCH, CANDIDATE_DIR)
    manager.handle_new_patches(final_files)
    manager.select_and_submit()
    manager.dump_state()

    logger.info(
        "Final: %d patches, %d validated, best=%s",
        len(manager.patches),
        len(manager.get_validated()),
        manager.best.path.name if manager.best else None,
    )

    logger.info("All patch CRSes done, exiting")


def run_patch_loop(crs, manager: EnsembleManager) -> None:
    """Main loop: fetch patches, validate, ensemble. Exits on ready signal."""
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)

    fetch_dir = os.environ.get("OSS_CRS_FETCH_DIR", "")
    if not fetch_dir:
        logger.error("OSS_CRS_FETCH_DIR is not set, cannot receive patches or ready signal")
        sys.exit(1)
    ready_file = Path(fetch_dir) / "status" / "ready"

    logger.info("Monitoring for patches (ready signal: %s)", ready_file)

    while True:
        try:
            new_files = crs.fetch(DataType.PATCH, CANDIDATE_DIR)
            manager.handle_new_patches(new_files)
        except Exception:
            logger.exception("Error in main loop, will retry")

        # Ready check outside try/except to prevent infinite loop on fetch errors
        try:
            if ready_file.exists():
                _handle_ready_signal(crs, manager)
                break
        except Exception:
            logger.exception("Error handling ready signal, exiting")
            break
        time.sleep(POLL_INTERVAL)


def main():
    logger.info("Starting patch ensemble: harness=%s", HARNESS)

    crs = init_libcrs()
    pov_files = fetch_povs(crs)
    crash_log_files, base_test_ok = run_baseline(crs, pov_files)
    setup_shared_dirs(crs)

    manager = EnsembleManager(crs, pov_files, crash_log_files, base_test_ok)
    manager.dump_state()

    run_patch_loop(crs, manager)


if __name__ == "__main__":
    main()
