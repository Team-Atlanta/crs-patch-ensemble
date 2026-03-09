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
SNAPSHOT_IMAGE = os.environ.get("OSS_CRS_SNAPSHOT_IMAGE", "")
HARNESS = os.environ.get("OSS_CRS_TARGET_HARNESS", "")
BUILDER_MODULE = os.environ.get("BUILDER_MODULE", "inc-builder-asan")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
SUBMISSION_FLUSH_WAIT_SECS = int(os.environ.get("SUBMISSION_FLUSH_WAIT_SECS", "12"))

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
    validated: bool = False


# --- Selector Setup ---


def setup_selector(work_dir: Path) -> None:
    """Configure Claude Code CLI for the selector.

    Sets LiteLLM proxy env vars, writes .claude.json to skip onboarding,
    and configures global gitignore.
    """
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

    # Write Claude JSON config to skip onboarding
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

    # Global gitignore so selection.json never leaks into diffs
    global_gitignore = Path.home() / ".gitignore"
    global_gitignore.write_text("selection.json\n")
    subprocess.run(
        ["git", "config", "--global", "core.excludesFile", str(global_gitignore)],
        capture_output=True,
    )


def reproduce_crashes(crs, pov_files: list[Path]) -> list[Path]:
    """Run each POV against the base (unpatched) build to collect crash logs.

    Returns a list of crash log file paths (one per POV).

    TODO: When there are many POVs, crash logs may be redundant. Consider
    deduplicating via a skill or subagent before passing to the selector.
    """
    crash_log_files = []
    for i, pov_path in enumerate(pov_files):
        response_dir = WORK_DIR / f"base_crash_{i}"
        response_dir.mkdir(parents=True, exist_ok=True)
        exit_code = crs.run_pov(
            pov_path, HARNESS, "base", response_dir, BUILDER_MODULE
        )
        stderr_path = response_dir / "pov_stderr.log"
        crash_log_files.append(stderr_path)
        log_size = stderr_path.stat().st_size if stderr_path.exists() else 0
        logger.info(
            "Base POV %d (%s): exit=%d log_size=%d",
            i, pov_path.name, exit_code, log_size,
        )
    return crash_log_files


# --- Ensemble Manager ---


class EnsembleManager:
    def __init__(self, crs, pov_files: list[Path], crash_log_files: list[Path]):
        self.crs = crs
        self.pov_files = pov_files
        self.crash_log_files = crash_log_files
        self.patches: dict[str, Patch] = {}
        self.best: Patch | None = None
        self._last_selection_set: frozenset[str] = frozenset()

    def handle_new_patches(self, new_files: list[str]) -> None:
        """Process new patch files: validate, decide ensemble, execute."""
        for fname in new_files:
            if fname in self.patches:
                continue
            patch_path = CANDIDATE_DIR / fname
            logger.info("New candidate: %s", fname)

            patch = Patch(path=patch_path, pov_total=len(self.pov_files))
            self.patches[fname] = patch
            self._validate(patch)

        if self._should_ensemble():
            self._ensemble()

    def _validate(self, patch: Patch) -> None:
        """Build, run all POVs, run test. Updates Patch in place."""
        # Build
        response_dir = WORK_DIR / "validate" / patch.path.stem / "build"
        response_dir.mkdir(parents=True, exist_ok=True)
        build_exit = self.crs.apply_patch_build(
            patch.path, response_dir, BUILDER_MODULE
        )
        if build_exit != 0:
            logger.info("Patch %s: build failed (exit=%d)", patch.path.name, build_exit)
            patch.validated = True
            return

        patch.build_ok = True
        build_id_file = response_dir / "build_id"
        if not build_id_file.exists():
            logger.warning("Patch %s: build ok but no build_id", patch.path.name)
            patch.validated = True
            return
        patch.build_id = build_id_file.read_text().strip()

        # Run POVs
        for pov_path in self.pov_files:
            pov_response = (
                WORK_DIR / "validate" / patch.path.stem / f"pov-{pov_path.stem}"
            )
            pov_response.mkdir(parents=True, exist_ok=True)
            pov_exit = self.crs.run_pov(
                pov_path, HARNESS, patch.build_id, pov_response, BUILDER_MODULE
            )
            passed = pov_exit == 0
            patch.pov_results[pov_path.name] = passed
            if passed:
                patch.pov_pass_count += 1

        logger.info(
            "Patch %s: POV %d/%d passed",
            patch.path.name, patch.pov_pass_count, patch.pov_total,
        )

        # Run test
        test_response = WORK_DIR / "validate" / patch.path.stem / "test"
        test_response.mkdir(parents=True, exist_ok=True)
        test_exit = self.crs.run_test(patch.build_id, test_response, BUILDER_MODULE)
        patch.test_ok = test_exit == 0

        patch.validated = True
        logger.info(
            "Patch %s: validation complete (build=%s pov=%d/%d test=%s)",
            patch.path.name, patch.build_ok,
            patch.pov_pass_count, patch.pov_total, patch.test_ok,
        )

    def _should_ensemble(self) -> bool:
        """Decide whether to run ensemble selection now.

        TODO: Replace with real strategy (e.g. wait for N patches, time-based).
        """
        return any(p.validated for p in self.patches.values())

    def _get_validated_patches(self) -> list[Patch]:
        """Return patches where build + all POVs + test all passed."""
        return [
            p for p in self.patches.values()
            if p.validated and p.build_ok
            and p.pov_pass_count == p.pov_total and p.pov_total > 0
            and p.test_ok
        ]

    def _ensemble(self) -> None:
        """Select the best patch and submit it."""
        validated = self._get_validated_patches()
        if not validated:
            logger.info("No fully validated patches (build+POV+test), skipping")
            return

        selected = self._ensemble_must_select(validated)
        if selected and (self.best is None or selected.path != self.best.path):
            self.best = selected
            logger.info("Submitting selected patch: %s", self.best.path.name)
            self.crs.submit(DataType.PATCH, self.best.path)

    def _ensemble_must_select(self, validated: list[Patch]) -> Patch | None:
        """Select the best patch from validated candidates.

        - 1 candidate: auto-select
        - 2+ candidates: use Claude Code to pick the most semantically correct fix
        """
        if len(validated) == 1:
            logger.info(
                "Single validated patch, auto-selecting: %s", validated[0].path.name
            )
            return validated[0]

        # Check if the validated set changed since last selection
        current_set = frozenset(p.path.name for p in validated)
        if current_set == self._last_selection_set:
            logger.info("Validated set unchanged, keeping current selection")
            return self.best
        self._last_selection_set = current_set

        logger.info(
            "Running selector on %d validated patches: %s",
            len(validated),
            [p.path.name for p in validated],
        )

        # Build label mapping
        label_to_patch: dict[str, Patch] = {}
        for i, patch in enumerate(validated):
            label_to_patch[LABELS[i]] = patch
            logger.info("Label %s -> %s", LABELS[i], patch.path.name)

        # Build selection prompt
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

        # Run Claude Code selector
        selected = self._run_selector(prompt, label_to_patch)
        if selected:
            return selected

        # Fallback: first validated patch
        logger.warning("Selector did not produce a valid selection, using first candidate")
        return validated[0]

    def _run_selector(
        self, prompt: str, label_to_patch: dict[str, Patch]
    ) -> Patch | None:
        """Invoke Claude Code CLI to select the best patch.

        Runs claude -p in SELECTOR_DIR, waits for it to write selection.json,
        and returns the corresponding Patch (or None on failure).
        """
        run_dir = SELECTOR_DIR / f"run_{int(time.time())}"
        run_dir.mkdir(parents=True, exist_ok=True)

        # Save prompt for debugging
        (run_dir / "prompt.txt").write_text(prompt)

        selection_file = run_dir / "selection.json"

        cmd = [
            "claude",
            "-p",
            "-d", str(run_dir),
            "--dangerously-skip-permissions",
        ]

        model = SELECTOR_MODEL or os.environ.get("ANTHROPIC_MODEL", "")
        if model:
            cmd.extend(["--model", model])

        debug_log = run_dir / "claude_debug.log"
        stdout_log = run_dir / "claude_stdout.log"
        stderr_log = run_dir / "claude_stderr.log"
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

        # Parse selection.json
        if not selection_file.exists():
            logger.warning("Selector did not produce selection.json")
            logger.info("Check logs at: %s", run_dir)
            return None

        raw = selection_file.read_text().strip()
        logger.info("Selector raw output: %s", raw)

        # Save selection result alongside other debug artifacts
        (run_dir / "selection_result.json").write_text(raw)

        selection = _parse_selection(raw)
        if selection in label_to_patch:
            patch = label_to_patch[selection]
            logger.info("Selector chose: label=%s patch=%s", selection, patch.path.name)
            return patch

        logger.warning("Selector returned invalid label: '%s'", selection)
        return None


def _parse_selection(raw: str) -> str:
    """Extract the 'selection' field from JSON, or return raw as fallback."""
    try:
        data = json.loads(raw)
        return data.get("selection", "").strip()
    except (json.JSONDecodeError, AttributeError):
        return raw.strip()


# --- Entry point ---


def wait_for_builder(crs) -> bool:
    try:
        domain = crs.get_service_domain(BUILDER_MODULE)
        logger.info("Builder sidecar '%s' resolved to %s", BUILDER_MODULE, domain)
        return True
    except RuntimeError as e:
        logger.error("Builder domain resolution failed: %s", e)
        return False


def main():
    logger.info("Starting patch ensemble: harness=%s", HARNESS)

    if not SNAPSHOT_IMAGE:
        logger.error("OSS_CRS_SNAPSHOT_IMAGE is not set.")
        sys.exit(1)

    crs = init_crs_utils()

    # Register patch submission directory
    PATCHES_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(
        target=crs.register_submit_dir,
        args=(DataType.PATCH, PATCHES_DIR),
        daemon=True,
    ).start()

    # Fetch POVs
    pov_files_fetched = crs.fetch(DataType.POV, POV_DIR)
    logger.info("Fetched %d POV(s)", len(pov_files_fetched))
    pov_files = sorted(
        f for f in POV_DIR.rglob("*")
        if f.is_file() and not f.name.startswith(".")
    )

    if not pov_files:
        logger.warning("No POV files found")
        sys.exit(0)

    if not wait_for_builder(crs):
        sys.exit(1)

    # Reproduce crashes on base build for selector context
    crash_log_files = reproduce_crashes(crs, pov_files)

    # Setup Claude Code for selector
    SELECTOR_DIR.mkdir(parents=True, exist_ok=True)
    setup_selector(SELECTOR_DIR)

    # Register Claude logs and selector workdir as shared dirs for post-run access
    claude_log_dir = Path.home() / ".claude"
    claude_log_dir.mkdir(parents=True, exist_ok=True)
    crs.register_shared_dir(claude_log_dir, "claude-logs")
    crs.register_shared_dir(SELECTOR_DIR, "selector")

    # Main loop
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    manager = EnsembleManager(crs, pov_files, crash_log_files)

    logger.info("Monitoring for patches...")

    while True:
        try:
            new_files = crs.fetch(DataType.PATCH, CANDIDATE_DIR)
            manager.handle_new_patches(new_files)
        except Exception:
            logger.exception("Error in main loop, will retry")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
