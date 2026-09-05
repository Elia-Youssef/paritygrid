/**
 * Phase 22 negative browser policy proof: the built shell runs under the
 * exact production Content Security Policy emitted by the static server
 * (cross-checked against `security_headers.py` by
 * `tests/api/test_csp_policy.py`), and the policy really blocks
 * prohibited script, style, frame, object, image, and connection
 * behavior.  Every probe asserts the `securitypolicyviolation` event
 * with the expected directive.  The inline-script and connection probes
 * additionally assert the blocked behavior itself (no execution, fetch
 * rejected), and the remote script, stylesheet, and image probes assert
 * the element-level load error that a blocked fetch produces.  The
 * frame/object probes rest on their violation events: probe hosts use
 * the reserved `.invalid` top-level domain, so a blocked load can never
 * succeed regardless.
 *
 * The lane stays offline.
 */
import { expect, test, type Page } from "@playwright/test";

const PROBE_ORIGIN = "https://csp-probe.invalid";

/** Wire-level production policy mirrors `security_headers.py`. */
const SHELL_CSP =
  "default-src 'none'; script-src 'self'; style-src 'self'; " +
  "img-src 'self'; font-src 'self'; connect-src 'self'; " +
  "object-src 'none'; frame-ancestors 'none'; base-uri 'none'; " +
  "form-action 'self'";
const DENY_ALL_CSP = "default-src 'none'; frame-ancestors 'none'";

interface CspViolation {
  directive: string;
  blocked: string;
}

/**
 * Resolves once an element-level load error proves the fetch itself was
 * blocked; combined with the violation event this asserts the blocked
 * effect, not only the report.
 */
async function expectBlockedLoad(page: Page): Promise<void> {
  await expect
    .poll(() =>
      page.evaluate(() => (window as unknown as Record<string, unknown>).__blockedLoad),
    )
    .toBeGreaterThan(0);
}

async function recordViolations(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const violations: { directive: string; blocked: string }[] = [];
    (window as unknown as Record<string, unknown>).__cspViolations = violations;
    (window as unknown as Record<string, unknown>).__blockedLoad = 0;
    document.addEventListener("securitypolicyviolation", (event) => {
      violations.push({
        directive: event.effectiveDirective,
        blocked: event.blockedURI,
      });
    });
  });
}

function violations(page: Page): Promise<CspViolation[]> {
  return page.evaluate(
    () =>
      (window as unknown as Record<string, unknown>).__cspViolations as CspViolation[],
  );
}

/**
 * Chromium reports the concrete CSP Level 3 directive that blocked the
 * load ("script-src-elem", "style-src-attr", ...), falling back to the
 * source directive.  The harness itself also injects one "eval" report
 * while compiling probe code, which is not an application violation, so
 * those records are ignored here.
 */
const SCRIPT_DIRECTIVES = ["script-src-elem", "script-src"];
const STYLE_DIRECTIVES = ["style-src-elem", "style-src"];

async function expectViolation(
  page: Page,
  directives: string[],
  blocked: string,
): Promise<CspViolation> {
  await expect
    .poll(async () => {
      const recorded = await violations(page);
      return recorded.some(
        (entry) =>
          directives.includes(entry.directive) &&
          entry.blocked !== "eval" &&
          entry.blocked.includes(blocked),
      );
    })
    .toBe(true);
  const recorded = await violations(page);
  return recorded.find(
    (entry) =>
      directives.includes(entry.directive) &&
      entry.blocked !== "eval" &&
      entry.blocked.includes(blocked),
  )!;
}

test.beforeEach(async ({ page }) => {
  await recordViolations(page);
  await page.goto("/app");
  await expect(page.getByRole("banner")).toBeVisible();
});

test("the shell and its packaged assets carry the production policies on the wire", async ({
  request,
}) => {
  const shell = await request.get("/app");
  expect(shell.status()).toBe(200);
  expect(shell.headers()["content-security-policy"]).toBe(SHELL_CSP);
  expect(shell.headers()["x-content-type-options"]).toBe("nosniff");

  const html = await shell.text();
  const assetPaths = [
    /src="(\/assets\/[^"]+\.js)"/.exec(html)?.[1],
    /href="(\/assets\/[^"]+\.css)"/.exec(html)?.[1],
  ];
  for (const assetPath of assetPaths) {
    expect(assetPath, "the shell must reference its hashed assets").toBeTruthy();
    const asset = await request.get(assetPath!);
    expect(asset.status()).toBe(200);
    expect(asset.headers()["content-security-policy"]).toBe(DENY_ALL_CSP);
    expect(asset.headers()["x-content-type-options"]).toBe("nosniff");
  }
});

test("inline script is blocked and never executes", async ({ page }) => {
  await page.evaluate(() => {
    const marker = document.createElement("script");
    marker.textContent = "window.__inlineRan = true;";
    document.body.append(marker);
  });
  await expectViolation(page, SCRIPT_DIRECTIVES, "inline");
  expect(
    await page.evaluate(
      () => (window as unknown as Record<string, unknown>).__inlineRan,
    ),
  ).toBeUndefined();
});

test("remote script is blocked", async ({ page }) => {
  await page.evaluate((origin) => {
    const remote = document.createElement("script");
    remote.addEventListener("error", () => {
      const state = window as unknown as Record<string, unknown>;
      state.__blockedLoad = Number(state.__blockedLoad) + 1;
    });
    remote.src = `${origin}/hostile.js`;
    document.body.append(remote);
  }, PROBE_ORIGIN);
  const violation = await expectViolation(
    page,
    SCRIPT_DIRECTIVES,
    `${PROBE_ORIGIN}/hostile.js`,
  );
  expect(violation.directive).toBeTruthy();
  await expectBlockedLoad(page);
});

test("frames and objects are blocked", async ({ page }) => {
  await page.evaluate((origin) => {
    const frame = document.createElement("iframe");
    frame.src = `${origin}/frame`;
    document.body.append(frame);
    const object = document.createElement("object");
    object.data = `${origin}/object`;
    document.body.append(object);
  }, PROBE_ORIGIN);
  // Chromium normalizes the blocked URL of navigational frames, so the
  // probe host is the stable identifier here, not the exact path.
  await expectViolation(page, ["frame-src"], PROBE_ORIGIN);
  await expectViolation(page, ["object-src"], PROBE_ORIGIN);
});

test("remote stylesheets and inline style elements are blocked", async ({ page }) => {
  await page.evaluate((origin) => {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.addEventListener("error", () => {
      const state = window as unknown as Record<string, unknown>;
      state.__blockedLoad = Number(state.__blockedLoad) + 1;
    });
    link.href = `${origin}/hostile.css`;
    document.head.append(link);
    const inline = document.createElement("style");
    inline.textContent = "body { background: #123456; }";
    document.head.append(inline);
  }, PROBE_ORIGIN);
  await expectViolation(page, STYLE_DIRECTIVES, `${PROBE_ORIGIN}/hostile.css`);
  await expectViolation(page, STYLE_DIRECTIVES, "inline");
  await expectBlockedLoad(page);
});

test("remote images are blocked", async ({ page }) => {
  await page.evaluate((origin) => {
    const image = document.createElement("img");
    image.addEventListener("error", () => {
      const state = window as unknown as Record<string, unknown>;
      state.__blockedLoad = Number(state.__blockedLoad) + 1;
    });
    image.src = `${origin}/exfiltration.png`;
    document.body.append(image);
  }, PROBE_ORIGIN);
  const violation = await expectViolation(
    page,
    ["img-src"],
    `${PROBE_ORIGIN}/exfiltration.png`,
  );
  expect(violation.directive).toBeTruthy();
  await expectBlockedLoad(page);
});

test("cross-origin connections are blocked", async ({ page }) => {
  const failure = page.evaluate(async (origin) => {
    try {
      await fetch(`${origin}/collect`, { mode: "no-cors" });
      return "fulfilled";
    } catch {
      return "rejected";
    }
  }, PROBE_ORIGIN);
  await expectViolation(page, ["connect-src"], `${PROBE_ORIGIN}/collect`);
  expect(await failure).toBe("rejected");
});
