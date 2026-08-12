"""Static checks for domain dependency boundaries."""

import argparse
import ast
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_PACKAGE_NAME = "paritygrid"
_DOMAIN_PACKAGE = "paritygrid.domain"
_ALLOWED_DOMAIN_MODULE_ROOTS = frozenset(
    {
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
        "builtins.open",
        "importlib.import_module",
    }
)
_INVALID_PYTHON = "<invalid-python>"
_MISSING_DOMAIN_ROOT = "<missing-domain-root>"


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


def _is_domain_import(imported_module: str) -> bool:
    return imported_module == _DOMAIN_PACKAGE or imported_module.startswith(f"{_DOMAIN_PACKAGE}.")


def _is_allowed_domain_import(imported_module: str) -> bool:
    if _is_domain_import(imported_module):
        return True
    module_root = imported_module.partition(".")[0]
    return module_root in _ALLOWED_DOMAIN_MODULE_ROOTS


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
    return aliases


def _qualified_call_name(node: ast.expr, aliases: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        if node.id in {"open", "__import__"}:
            return f"builtins.{node.id}"
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        owner = _qualified_call_name(node.value, aliases)
        if owner is not None:
            return f"{owner}.{node.attr}"
    return None


def _find_file_violations(path: Path, package_root: Path) -> list[ImportViolation]:
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
            base = _resolve_from_import(node, module_name, is_package)
            imported_modules.extend(
                f"{base}.{alias.name}" if base else alias.name for alias in node.names
            )
            import_line = node.lineno
        if import_line is not None:
            violations.extend(
                ImportViolation(path=path, line=import_line, imported_module=imported_module)
                for imported_module in imported_modules
                if not _is_allowed_domain_import(imported_module)
            )
        if isinstance(node, ast.Call):
            call_name = _qualified_call_name(node.func, aliases)
            if call_name in _FORBIDDEN_CALLS:
                violations.append(
                    ImportViolation(path=path, line=node.lineno, imported_module=call_name)
                )
    return violations


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
    violations = find_domain_import_violations(source_root)
    if not violations:
        print("Import boundaries passed.")
        return 0
    for violation in violations:
        print(violation.render(source_root), file=sys.stderr)
    return 1
