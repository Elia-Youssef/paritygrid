/**
 * Phase 20 canonical demo backend harness (fixture helper, zero mocks).
 *
 * Spawns the REAL packaged Python demo backend behind the `paritygrid demo`
 * CLI and owns its full lifecycle for one browser-worker run:
 *
 * 1. Story pre-execution: three complete headless demo invocations
 *    (`demo --root <root> --headless --runner sequential|threaded|asyncio`)
 *    over one fresh owned demo root. This is the accepted way a demo root
 *    comes to hold the canonical story run (`run_canonical-demo`, succeeded,
 *    with its reconciliation snapshot, conflicts, and the applied repair
 *    plan) side by side with the three canonical engine runs
 *    (`run_can-engine-0001..0003`, equal execution-evidence fingerprints).
 * 2. Serve mode: `demo --root <root>` (NO --headless) registers the canonical
 *    connectors and publishes the canonical pipeline (idempotently — the
 *    headless phases already published it), starts the loopback simulators
 *    and the packaged application on a dynamic 127.0.0.1 port, prints
 *    `Serving the packaged application at http://127.0.0.1:<port>/`, starts
 *    the serve-mode run launcher, and keeps serving until killed. The URL is
 *    parsed from that stdout line; nothing else about the port is assumed.
 * 3. Discovery of the canonical repair plan id. The public API has no
 *    "list plans for a run" route (`POST /runs/{id}/repair-plans` creates,
 *    `GET /repair-plans/{id}` reads by id), so the id is read from the
 *    demo root's durable SQLite store with the repository's own Python —
 *    a real read of committed data, never a mock or a fixture.
 * 4. Termination: the serve process is killed as a TREE and its exit is
 *    asserted. On Windows `paritygrid.exe` is a launcher shim whose real
 *    Python server (plus further descendants) dies only with a tree kill
 *    (`taskkill /T /F`); a bare `child.kill()` would orphan the server.
 *    The demo root must still exist afterwards: shutdown never deletes
 *    data. The harness then exercises `demo-reset` and requires that exact
 *    owned root to disappear, preventing test-owned state from accumulating.
 *
 * There are no fixed sleeps anywhere: every wait polls an observable fact
 * (process exit, an arriving stdout line, a spawn error) under a deadline.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

/** The demo root holds the packaged app's SQLite store under `scenario/`. */
const DATABASE_RELPATH = path.join("scenario", "canonical.db");
const CANONICAL_STORY_RUN_ID = "run_canonical-demo";
const SERVE_URL_LINE_PREFIX = "Serving the packaged application at";
const HEADLESS_RUNNERS = ["sequential", "threaded", "asyncio"] as const;

const HEADLESS_EXIT_TIMEOUT_MS = 120_000;
const SERVE_URL_TIMEOUT_MS = 120_000;
const PLAN_DISCOVERY_TIMEOUT_MS = 30_000;
const EXIT_AFTER_KILL_TIMEOUT_MS = 20_000;
/** Captured child output kept for diagnostics (tail only). */
const OUTPUT_TAIL_BYTES = 65_536;

/**
 * The repository root. This file lives in `<repo>/web/e2e`; e2e runs with
 * cwd set to `<repo>/web`, so both derivations should agree. The candidate
 * that carries the Python workspace marker wins.
 */
function resolveRepoRoot(): string {
  const moduleDir =
    typeof __dirname === "string"
      ? __dirname
      : path.dirname(fileURLToPath(import.meta.url));
  const candidates = [
    path.resolve(moduleDir, "..", ".."),
    path.resolve(process.cwd(), ".."),
  ];
  for (const candidate of candidates) {
    if (existsSync(path.join(candidate, "pyproject.toml"))) {
      return candidate;
    }
  }
  throw new Error(
    `cannot locate the ParityGrid repository root (tried ${candidates.join(", ")})`,
  );
}

const REPO_ROOT = resolveRepoRoot();
const IS_WINDOWS = process.platform === "win32";
const DEMO_BINARY = IS_WINDOWS
  ? path.join(REPO_ROOT, ".venv", "Scripts", "paritygrid.exe")
  : path.join(REPO_ROOT, ".venv", "bin", "paritygrid");
const DEMO_PYTHON = IS_WINDOWS
  ? path.join(REPO_ROOT, ".venv", "Scripts", "python.exe")
  : path.join(REPO_ROOT, ".venv", "bin", "python");

export interface DemoBackend {
  /** Base URL of the REAL packaged application, e.g. http://127.0.0.1:52928/ */
  readonly baseUrl: string;
  /** Absolute path of the owned demo root (created fresh for this run). */
  readonly demoRoot: string;
  /** Durable plan id of the canonical story's applied repair plan. */
  readonly repairPlanId: string;
  /**
   * Stop the backend: kill the serve process tree, assert the process and
   * listener exited without deleting data, then safely reset the exact owned
   * root. Throws when any assertion fails.
   */
  stop(): Promise<void>;
}

/** Bound, tail-keeping collector for a child's stdout/stderr diagnostics. */
class OutputCapture {
  private buffer = "";

  readonly handler = (chunk: Buffer): void => {
    const combined = this.buffer + chunk.toString("utf8");
    this.buffer =
      combined.length > OUTPUT_TAIL_BYTES
        ? combined.slice(-OUTPUT_TAIL_BYTES)
        : combined;
  };

  text(): string {
    return this.buffer;
  }
}

interface SpawnedProcess {
  readonly child: ChildProcess;
  readonly stdout: OutputCapture;
  readonly stderr: OutputCapture;
}

function spawnDemo(args: string[]): SpawnedProcess {
  if (!existsSync(DEMO_BINARY)) {
    throw new Error(
      `the demo binary is missing; build the Python package first: ${DEMO_BINARY}`,
    );
  }
  const stdout = new OutputCapture();
  const stderr = new OutputCapture();
  const child = spawn(DEMO_BINARY, args, {
    cwd: REPO_ROOT,
    env: { ...process.env, PYTHONUNBUFFERED: "1" },
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  child.stdout?.on("data", stdout.handler);
  child.stderr?.on("data", stderr.handler);
  return { child, stdout, stderr };
}

/**
 * Kill the whole process tree of a spawned demo process. On Windows the
 * `paritygrid.exe` console-script shim owns the real Python server as a
 * descendant, so only a tree kill reaches the listening server.
 */
function killProcessTree(child: ChildProcess): void {
  if (child.pid === undefined || child.exitCode !== null) {
    return;
  }
  if (IS_WINDOWS) {
    spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], {
      stdio: "ignore",
      windowsHide: true,
    });
    return;
  }
  try {
    child.kill("SIGTERM");
  } catch {
    // The child may already be gone; the exit assertion decides the outcome.
  }
}

/** Wait for the child's exit event under a deadline; false on timeout. */
function waitForExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve(true);
  }
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

/** Run one complete headless demo lifecycle and require its exit code 0. */
async function runHeadlessDemo(demoRoot: string, runner: string): Promise<void> {
  const spawned = spawnDemo([
    "demo",
    "--root",
    demoRoot,
    "--headless",
    "--runner",
    runner,
  ]);
  const exited = await waitForExit(spawned.child, HEADLESS_EXIT_TIMEOUT_MS);
  if (!exited) {
    killProcessTree(spawned.child);
    await waitForExit(spawned.child, EXIT_AFTER_KILL_TIMEOUT_MS);
    throw new Error(
      `the headless ${runner} demo did not finish within ${String(HEADLESS_EXIT_TIMEOUT_MS)}ms\n` +
        `stdout tail:\n${spawned.stdout.text()}\n` +
        `stderr tail:\n${spawned.stderr.text()}`,
    );
  }
  if (spawned.child.exitCode !== 0) {
    throw new Error(
      `the headless ${runner} demo exited with code ${String(spawned.child.exitCode)}\n` +
        `stdout tail:\n${spawned.stdout.text()}\n` +
        `stderr tail:\n${spawned.stderr.text()}`,
    );
  }
}

/** Reset one exact owned demo root through the public CLI and require success. */
async function resetDemoRoot(demoRoot: string): Promise<void> {
  const spawned = spawnDemo(["demo-reset", "--root", demoRoot]);
  const exited = await waitForExit(spawned.child, HEADLESS_EXIT_TIMEOUT_MS);
  if (!exited) {
    killProcessTree(spawned.child);
    await waitForExit(spawned.child, EXIT_AFTER_KILL_TIMEOUT_MS);
    throw new Error(
      `demo-reset did not finish within ${String(HEADLESS_EXIT_TIMEOUT_MS)}ms\n` +
        `stdout tail:\n${spawned.stdout.text()}\n` +
        `stderr tail:\n${spawned.stderr.text()}`,
    );
  }
  if (spawned.child.exitCode !== 0) {
    throw new Error(
      `demo-reset exited with code ${String(spawned.child.exitCode)}\n` +
        `stdout tail:\n${spawned.stdout.text()}\n` +
        `stderr tail:\n${spawned.stderr.text()}`,
    );
  }
}

interface ServeHandle {
  readonly url: string;
  readonly spawned: SpawnedProcess;
}

/**
 * Start serve mode and resolve with the packaged application's URL as soon
 * as the demo announces it on stdout. Diagnostics keep flowing into the
 * capture afterwards so a full pipe can never block the server.
 */
async function startServeMode(demoRoot: string): Promise<ServeHandle> {
  const spawned = spawnDemo(["demo", "--root", demoRoot]);
  return await new Promise<ServeHandle>((resolve, reject) => {
    let pending = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      killProcessTree(spawned.child);
      void waitForExit(spawned.child, EXIT_AFTER_KILL_TIMEOUT_MS);
      reject(
        new Error(
          `the demo serve mode never announced its URL within ${String(SERVE_URL_TIMEOUT_MS)}ms\n` +
            `stdout tail:\n${spawned.stdout.text()}\n` +
            `stderr tail:\n${spawned.stderr.text()}`,
        ),
      );
    }, SERVE_URL_TIMEOUT_MS);

    const onStdout = (chunk: Buffer): void => {
      if (settled) {
        return;
      }
      pending = (pending + chunk.toString("utf8")).slice(-OUTPUT_TAIL_BYTES);
      const lines = pending.split(/\r?\n/);
      pending = lines.pop() ?? "";
      for (const line of lines) {
        if (line.startsWith(SERVE_URL_LINE_PREFIX)) {
          settled = true;
          clearTimeout(timer);
          spawned.child.off("exit", onExit);
          spawned.child.off("error", onError);
          resolve({
            url: line.slice(SERVE_URL_LINE_PREFIX.length).trim(),
            spawned,
          });
          return;
        }
      }
    };
    const onExit = (code: number | null): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      reject(
        new Error(
          `the demo serve mode exited before announcing its URL (code ${String(code)})\n` +
            `stdout tail:\n${spawned.stdout.text()}\n` +
            `stderr tail:\n${spawned.stderr.text()}`,
        ),
      );
    };
    const onError = (error: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      reject(new Error(`the demo serve process failed to spawn: ${String(error)}`));
    };

    spawned.child.stdout?.on("data", onStdout);
    spawned.child.once("exit", onExit);
    spawned.child.once("error", onError);
  });
}

/** Read the canonical repair plan id from the demo root's durable store. */
async function discoverCanonicalRepairPlanId(demoRoot: string): Promise<string> {
  const databasePath = path.join(demoRoot, DATABASE_RELPATH);
  const query = [
    "import json, sqlite3, sys",
    "connection = sqlite3.connect(sys.argv[1])",
    "rows = connection.execute(",
    "    'select repair_plan_id from repair_plans where run_id = ? order by repair_plan_id',",
    "    (sys.argv[2],),",
    ").fetchall()",
    "connection.close()",
    "print(json.dumps([row[0] for row in rows]))",
  ].join("\n");
  return await new Promise<string>((resolve, reject) => {
    const stdout = new OutputCapture();
    const stderr = new OutputCapture();
    const child = spawn(
      DEMO_PYTHON,
      ["-c", query, databasePath, CANONICAL_STORY_RUN_ID],
      {
        cwd: REPO_ROOT,
        env: { ...process.env },
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      },
    );
    child.stdout?.on("data", stdout.handler);
    child.stderr?.on("data", stderr.handler);
    const timer = setTimeout(() => {
      killProcessTree(child);
      reject(
        new Error(
          `repair-plan discovery did not finish within ${String(PLAN_DISCOVERY_TIMEOUT_MS)}ms\n` +
            `stderr tail:\n${stderr.text()}`,
        ),
      );
    }, PLAN_DISCOVERY_TIMEOUT_MS);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(new Error(`repair-plan discovery failed to spawn: ${String(error)}`));
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(
          new Error(
            `repair-plan discovery exited with code ${String(code)}\n` +
              `stderr tail:\n${stderr.text()}`,
          ),
        );
        return;
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(stdout.text().trim()) as unknown;
      } catch (error) {
        reject(
          new Error(
            `repair-plan discovery produced unreadable output: ${String(error)}\n` +
              `stdout tail:\n${stdout.text()}`,
          ),
        );
        return;
      }
      if (
        !Array.isArray(parsed) ||
        parsed.length !== 1 ||
        typeof parsed[0] !== "string"
      ) {
        const count = Array.isArray(parsed) ? String(parsed.length) : "unreadable";
        reject(
          new Error(
            `the demo root holds ${count} repair plans for ${CANONICAL_STORY_RUN_ID}; ` +
              "exactly one is expected",
          ),
        );
        return;
      }
      resolve(parsed[0]);
    });
  });
}

/**
 * Start the complete canonical demo backend for one browser run: three
 * headless story phases over one fresh demo root, then serve mode with the
 * run launcher, then plan discovery. Any phase failure aborts startup with
 * the captured output attached.
 */
export async function startDemoBackend(): Promise<DemoBackend> {
  const demoRoot = path.join(
    os.tmpdir(),
    `paritygrid-e2e-${String(process.pid)}-${String(Date.now())}`,
  );
  for (const runner of HEADLESS_RUNNERS) {
    await runHeadlessDemo(demoRoot, runner);
  }
  const serve = await startServeMode(demoRoot);
  const baseUrl = new URL(serve.url).toString();
  const repairPlanId = await discoverCanonicalRepairPlanId(demoRoot);
  let stopped = false;
  return {
    baseUrl,
    demoRoot,
    repairPlanId,
    async stop(): Promise<void> {
      if (stopped) {
        throw new Error("the demo backend was already stopped");
      }
      stopped = true;
      const child = serve.spawned.child;
      const alreadyExited = await waitForExit(child, 0);
      if (!alreadyExited) {
        killProcessTree(child);
      }
      const exited = await waitForExit(child, EXIT_AFTER_KILL_TIMEOUT_MS);
      if (!exited) {
        throw new Error(
          "the demo serve process did not exit after the tree kill\n" +
            `stdout tail:\n${serve.spawned.stdout.text()}\n` +
            `stderr tail:\n${serve.spawned.stderr.text()}`,
        );
      }
      // The tree kill must have reached the listening server itself, not
      // only the launcher shim: the port must stop answering health probes.
      try {
        await fetch(new URL("healthz", baseUrl), {
          signal: AbortSignal.timeout(5_000),
        });
        throw new Error(
          "the demo application still answers after the serve process exited; " +
            "the process tree was not fully terminated",
        );
      } catch (error) {
        // `fetch` rejects with a TypeError when the connection is refused —
        // the expected post-shutdown observation. Anything else is fatal.
        if (!(error instanceof TypeError)) {
          throw error;
        }
      }
      if (!existsSync(demoRoot)) {
        throw new Error(
          `the demo root vanished during shutdown; the demo must never delete data: ${demoRoot}`,
        );
      }
      await resetDemoRoot(demoRoot);
      if (existsSync(demoRoot)) {
        throw new Error(`demo-reset left the exact owned root behind: ${demoRoot}`);
      }
      const launcherDiagnostics = serve.spawned.stderr
        .text()
        .split(/\r?\n/)
        .filter((line) => line.startsWith("[demo-launcher]"));
      if (launcherDiagnostics.length > 0) {
        throw new Error(
          `the demo launcher reported execution failures:\n${launcherDiagnostics.join("\n")}`,
        );
      }
    },
  };
}
