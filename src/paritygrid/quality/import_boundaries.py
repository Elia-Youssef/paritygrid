"""Static checks for dependency and process-isolation boundaries."""

import argparse
import ast
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

_PACKAGE_NAME = "paritygrid"
_DOMAIN_PACKAGE = "paritygrid.domain"
_ALLOWED_DOMAIN_MODULE_ROOTS = frozenset(
    {
        "__future__",
        "collections",
        "dataclasses",
        "datetime",
        "decimal",
        "enum",
        "hashlib",
        "json",
        "re",
        "types",
        "typing",
        "unicodedata",
    }
)
_FORBIDDEN_CALLS = frozenset(
    {
        "builtins.__import__",
        "builtins.compile",
        "builtins.eval",
        "builtins.exec",
        "builtins.globals",
        "builtins.locals",
        "builtins.open",
        "builtins.vars",
        "importlib.import_module",
    }
)
_BUILTIN_CALL_NAMES = frozenset(
    {
        "__import__",
        "compile",
        "eval",
        "exec",
        "globals",
        "locals",
        "open",
        "vars",
    }
)
_DANGEROUS_RUNTIME_REFERENCES = _BUILTIN_CALL_NAMES | {
    "__builtins__",
    "__loader__",
    "__spec__",
}
_INVALID_PYTHON = "<invalid-python>"
_MISSING_DOMAIN_ROOT = "<missing-domain-root>"
_RELATIVE_IMPORT_ESCAPE = "<relative-import-escape>"
_PROCESS_WORKER_FORBIDDEN_PREFIXES = (
    "alembic",
    "aiohttp",
    "anyio",
    "asyncio",
    "builtins",
    "concurrent.futures",
    "dbm",
    "duckdb",
    "fileinput",
    "ftplib",
    "glob",
    "http",
    "httpx",
    "imaplib",
    "importlib",
    "io",
    "mmap",
    "multiprocessing",
    "os",
    "pathlib",
    "pkgutil",
    "poplib",
    "requests",
    "runpy",
    "shelve",
    "shutil",
    "smtplib",
    "socket",
    "socketserver",
    "sqlite3",
    "sqlalchemy",
    "ssl",
    "subprocess",
    "tarfile",
    "tempfile",
    "threading",
    "urllib",
    "websockets",
    "xmlrpc",
    "zipfile",
    "zipimport",
    "paritygrid.adapters.analytics",
    "paritygrid.adapters.artifacts",
    "paritygrid.adapters.connectors",
    "paritygrid.adapters.persistence",
    "paritygrid.api",
    "paritygrid.application.execution",
    "paritygrid.application.execution.checkpoint_commit",
    "paritygrid.application.execution.finalization",
    "paritygrid.application.execution.leasing",
    "paritygrid.application.execution.recovery",
    "paritygrid.application.execution.result_sink",
    "paritygrid.application.ports",
    "paritygrid.application.writes",
    "paritygrid.cli",
    "paritygrid.runtime",
)


@dataclass(frozen=True, slots=True, order=True)
class ImportViolation:
    """One import that crosses from the domain into an outer package."""

    path: Path
    line: int
    imported_module: str

    def render(self, root: Path) -> str:
        """Render a stable repository-relative diagnostic."""
        return f"{self.path.relative_to(root).as_posix()}:{self.line}: {self.imported_module}"


def _module_name(path: Path, package_root: Path) -> str:
    relative = path.relative_to(package_root.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_from_import(node: ast.ImportFrom, current_module: str, is_package: bool) -> str:
    if node.level == 0:
        return node.module or ""
    current_parts = current_module.split(".")
    package_parts = current_parts if is_package else current_parts[:-1]
    keep = len(package_parts) - (node.level - 1)
    base_parts = package_parts[: max(keep, 0)]
    if node.module:
        base_parts.extend(node.module.split("."))
    return ".".join(base_parts)


def _is_relative_import_escape(node: ast.ImportFrom, current_module: str, is_package: bool) -> bool:
    if node.level == 0:
        return False
    current_parts = current_module.split(".")
    package_depth = len(current_parts) if is_package else len(current_parts) - 1
    return node.level > package_depth


def _is_domain_import(imported_module: str) -> bool:
    return imported_module == _DOMAIN_PACKAGE or imported_module.startswith(f"{_DOMAIN_PACKAGE}.")


def _is_allowed_domain_import(imported_module: str) -> bool:
    if _is_domain_import(imported_module):
        return True
    module_root = imported_module.partition(".")[0]
    return module_root in _ALLOWED_DOMAIN_MODULE_ROOTS


def _is_allowed_process_worker_import(imported_module: str) -> bool:
    if imported_module == _RELATIVE_IMPORT_ESCAPE:
        return False
    return not any(
        imported_module == prefix or imported_module.startswith(f"{prefix}.")
        for prefix in _PROCESS_WORKER_FORBIDDEN_PREFIXES
    )


def _import_aliases(tree: ast.AST, current_module: str, is_package: bool) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.partition(".")[0]
                aliases[local_name] = alias.name if alias.asname else local_name
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_from_import(node, current_module, is_package)
            for alias in node.names:
                local_name = alias.asname or alias.name
                aliases[local_name] = f"{base}.{alias.name}" if base else alias.name
    assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign | ast.AnnAssign | ast.NamedExpr)
    ]
    for _ in range(len(assignments) + 1):
        previous = aliases.copy()
        for node in assignments:
            value, targets = _assignment_parts(node)
            if value is None:
                continue
            qualified_value = _qualified_call_name(value, aliases)
            if qualified_value is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    aliases[target.id] = qualified_value
        if aliases == previous:
            break
    return aliases


def _assignment_parts(
    node: ast.Assign | ast.AnnAssign | ast.NamedExpr,
) -> tuple[ast.expr | None, tuple[ast.expr, ...]]:
    if isinstance(node, ast.Assign):
        return node.value, tuple(node.targets)
    return node.value, (node.target,)


def _qualified_call_name(node: ast.expr, aliases: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        if node.id in aliases:
            return aliases[node.id]
        if node.id == "__builtins__":
            return "builtins"
        if node.id in _BUILTIN_CALL_NAMES:
            return f"builtins.{node.id}"
        return node.id
    if isinstance(node, ast.Attribute):
        owner = _qualified_call_name(node.value, aliases)
        if owner is not None:
            return f"{owner}.{node.attr}"
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        owner = _qualified_call_name(node.value, aliases)
        if owner is not None and isinstance(node.slice.value, str):
            return f"{owner}.{node.slice.value}"
    if isinstance(node, ast.Call):
        accessor = _qualified_call_name(node.func, aliases)
        if accessor in {"builtins.getattr", "getattr"} and len(node.args) >= 2:
            owner = _qualified_call_name(node.args[0], aliases)
            attribute = node.args[1]
            if (
                owner is not None
                and isinstance(attribute, ast.Constant)
                and isinstance(attribute.value, str)
            ):
                return f"{owner}.{attribute.value}"
    return None


def _find_file_violations(
    path: Path,
    package_root: Path,
    *,
    is_allowed_import: Callable[[str], bool] = _is_allowed_domain_import,
) -> list[ImportViolation]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeError) as error:
        line = error.lineno if isinstance(error, SyntaxError) and error.lineno is not None else 1
        return [ImportViolation(path=path, line=line, imported_module=_INVALID_PYTHON)]
    module_name = _module_name(path, package_root)
    is_package = path.name == "__init__.py"
    aliases = _import_aliases(tree, module_name, is_package)
    violations: list[ImportViolation] = []

    for node in ast.walk(tree):
        imported_modules: list[str] = []
        import_line: int | None = None
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
            import_line = node.lineno
        elif isinstance(node, ast.ImportFrom):
            if _is_relative_import_escape(node, module_name, is_package):
                imported_modules.append(_RELATIVE_IMPORT_ESCAPE)
            else:
                base = _resolve_from_import(node, module_name, is_package)
                imported_modules.extend(
                    f"{base}.{alias.name}" if base else alias.name for alias in node.names
                )
            import_line = node.lineno
        if import_line is not None:
            violations.extend(
                ImportViolation(path=path, line=import_line, imported_module=imported_module)
                for imported_module in imported_modules
                if not is_allowed_import(imported_module)
            )
        if isinstance(node, ast.Call):
            call_name = _qualified_call_name(node.func, aliases)
            if call_name in _FORBIDDEN_CALLS:
                violations.append(
                    ImportViolation(path=path, line=node.lineno, imported_module=call_name)
                )
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id in _DANGEROUS_RUNTIME_REFERENCES
        ):
            if node.id in {"__loader__", "__spec__"}:
                reference_name = f"runtime.{node.id}"
            else:
                reference_name = "builtins" if node.id == "__builtins__" else f"builtins.{node.id}"
            violations.append(
                ImportViolation(
                    path=path,
                    line=node.lineno,
                    imported_module=reference_name,
                )
            )
    return sorted(set(violations))


def find_domain_import_violations(source_root: Path) -> tuple[ImportViolation, ...]:
    """Find imports and calls that violate the pure-domain boundary."""
    package_root = source_root / _PACKAGE_NAME
    domain_root = package_root / "domain"
    if not domain_root.is_dir():
        return (
            ImportViolation(
                path=domain_root,
                line=0,
                imported_module=_MISSING_DOMAIN_ROOT,
            ),
        )
    violations = [
        violation
        for path in sorted(domain_root.rglob("*.py"))
        for violation in _find_file_violations(path, package_root)
    ]
    return tuple(sorted(violations))


def find_process_worker_import_violations(source_root: Path) -> tuple[ImportViolation, ...]:
    """Reject persistence, write, filesystem, and dynamic access in process workers."""
    package_root = source_root / _PACKAGE_NAME
    worker_root = package_root / "adapters" / "runners" / "process_workers"
    if not worker_root.exists():
        return ()
    violations = [
        violation
        for path in sorted(worker_root.rglob("*.py"))
        for violation in _find_file_violations(
            path,
            package_root,
            is_allowed_import=_is_allowed_process_worker_import,
        )
    ]
    return tuple(sorted(violations))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the import-boundary check and return a process status code."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("src"),
        help="Directory containing the paritygrid package.",
    )
    arguments = parser.parse_args(argv)
    source_root = arguments.source_root.resolve()
    violations = (
        *find_domain_import_violations(source_root),
        *find_process_worker_import_violations(source_root),
    )
    if not violations:
        print("Import boundaries passed.")
        return 0
    for violation in violations:
        print(violation.render(source_root), file=sys.stderr)
    return 1
