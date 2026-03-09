"""
crs-patch-ensemble patcher module.

Monitors exchange directory for patches from other CRSs,
validates each patch (build + POV + test), and submits the best one.
"""

import logging
import os
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

# Directories
WORK_DIR = Path("/work")
PATCHES_DIR = Path("/patches")
CANDIDATE_DIR = WORK_DIR / "candidates"
POV_DIR = WORK_DIR / "povs"


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


# --- Ensemble Manager ---


class EnsembleManager:
    def __init__(self, crs, pov_files: list[Path]):
        self.crs = crs
        self.pov_files = pov_files
        self.patches: dict[str, Patch] = {}
        self.best: Patch | None = None

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

    def _should_ensemble(self) -> bool:
        """Decide whether to run ensemble selection now.

        TODO: Replace with real strategy (e.g. wait for N patches, time-based).
        """
        # Fake: run ensemble whenever there's any validated patch
        return any(p.validated for p in self.patches.values())

    def _ensemble(self) -> None:
        """Select the best patch and submit it.

        TODO: Replace with LLM-based evaluation.
        """
        validated = [p for p in self.patches.values() if p.validated and p.build_ok]
        if not validated:
            return

        def score(p: Patch):
            return (p.pov_pass_count, p.test_ok, p.build_ok)

        candidate = max(validated, key=score)

        if self.best is None or score(candidate) > score(self.best):
            self.best = candidate
            logger.info(
                "New best: %s (pov=%d/%d test=%s)",
                self.best.path.name,
                self.best.pov_pass_count,
                self.best.pov_total,
                self.best.test_ok,
            )
            self.crs.submit(DataType.PATCH, self.best.path)


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

    # Main loop
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    manager = EnsembleManager(crs, pov_files)

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
