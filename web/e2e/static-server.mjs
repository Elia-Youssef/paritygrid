/**
 * Static file server for the browser suites.
 *
 * The previous lane served `dist/` through `vite preview`, whose server
 * process dies on an unhandled socket `error` when a browser aborts a
 * keep-alive connection mid-flight — a timing only WebKit produces
 * reliably, and which then failed every later test in the worker with
 * "Could not connect to server".  This server serves the same built
 * files with the SPA fallback the shell expects and swallows client
 * aborts, so one cancelled request can never take the lane down.
 *
 * Stdlib only; fails loudly when the port is taken so Playwright never
 * silently reuses a stale server.
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, normalize, resolve, sep } from "node:path";
import process from "node:process";

const host = "127.0.0.1";
const port = Number(process.env.PORT ?? 4173);
const distRoot = resolve(import.meta.dirname, "..", "dist");

const contentTypes = new Map([
    [".css", "text/css; charset=utf-8"],
    [".html", "text/html; charset=utf-8"],
    [".ico", "image/x-icon"],
    [".js", "text/javascript; charset=utf-8"],
    [".json", "application/json; charset=utf-8"],
    [".map", "application/json; charset=utf-8"],
    [".svg", "image/svg+xml"],
    [".woff", "font/woff"],
    [".woff2", "font/woff2"],
]);

// The exact production policies from
// `src/paritygrid/api/middleware/security_headers.py`, so the whole
// browser lane runs under the shipped Content Security Policy and any
// policy-breaking change in the shell fails this lane too.
const shellCsp =
    "default-src 'none'; script-src 'self'; style-src 'self'; " +
    "img-src 'self'; font-src 'self'; connect-src 'self'; " +
    "object-src 'none'; frame-ancestors 'none'; base-uri 'none'; " +
    "form-action 'self'";
const denyAllCsp = "default-src 'none'; frame-ancestors 'none'";

function securityHeaders(contentType) {
    const csp = contentType.startsWith("text/html") ? shellCsp : denyAllCsp;
    return {
        "content-security-policy": csp,
        "x-content-type-options": "nosniff",
        "x-frame-options": "DENY",
        "referrer-policy": "no-referrer",
        "cross-origin-opener-policy": "same-origin",
    };
}

async function serve(pathname, response) {
    const normalized = normalize(decodeURIComponent(pathname)).replace(
        /^(\.\.[/\\])+/,
        "",
    );
    const candidates = [];
    if (normalized === "/" || normalized === "") {
        candidates.push("index.html");
    } else {
        const withoutLeading = normalized.replace(/^[\\/]+/, "");
        candidates.push(withoutLeading);
        // Deep links such as /app/runs fall back to the shell entry point.
        candidates.push("index.html");
    }
    for (const candidate of candidates) {
        const filePath = resolve(distRoot, candidate);
        if (filePath !== distRoot && !filePath.startsWith(`${distRoot}${sep}`)) {
            continue;
        }
        try {
            const body = await readFile(filePath);
            const contentType =
                contentTypes.get(extname(candidate)) ?? "application/octet-stream";
            response.writeHead(200, {
                "content-type": contentType,
                ...securityHeaders(contentType),
            });
            response.end(body);
            return true;
        } catch {
            // Try the next candidate; the SPA fallback is last.
        }
    }
    return false;
}

const server = createServer((request, response) => {
    // A client that walks away mid-response must never reach an unhandled
    // stream error; destroy the socket quietly instead.
    response.on("error", () => response.destroy());
    request.on("error", () => response.destroy());
    void serve(new URL(request.url ?? "/", `http://${host}`).pathname, response).catch(
        () => response.destroy(),
    );
});
// Keep-alive sockets aborted by the browser emit 'error' with no other
// listener; swallow them so the server outlives any single client.
server.on("connection", (socket) => {
    socket.on("error", () => socket.destroy());
});

server.on("error", (error) => {
    console.error(`static e2e server failed: ${String(error)}`);
    process.exit(1);
});

server.listen(port, host, () => {
    console.log(`static e2e server on http://${host}:${port}/`);
});
