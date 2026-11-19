"""Check production import direction and the absence of adapter cycles."""

import ast
import sys
from graphlib import TopologicalSorter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def production_module_imports() -> dict[str, set[str]]:
    """Read absolute imports, including local imports deferred until function calls."""
    paths = [ROOT / "app.py"]
    for layer in ("api", "common", "config", "domain", "runtime", "services", "storage"):
        paths.extend((ROOT / layer).glob("*.py"))
    imports = {}
    for path in paths:
        module = ".".join(path.relative_to(ROOT.parent).with_suffix("").parts)
        dependencies = set()
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                dependencies.update(name.name for name in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert node.level == 0
                if node.module is not None:
                    dependencies.add(node.module)
                    if node.module.startswith("control_plane."):
                        dependencies.update(f"{node.module}.{name.name}" for name in node.names)
        imports[module] = dependencies
    return imports


def test_layers_depend_on_contracts_without_import_cycles():
    """Keep HTTP, Docker and SQLite adapters out of the layers that use their contracts."""
    graph = production_module_imports()
    topology = TopologicalSorter({
        module: dependencies & graph.keys() for module, dependencies in graph.items()
    })
    assert set(topology.static_order()) == graph.keys()
    for module, imports in graph.items():
        layer = module.split(".")[1]
        if layer == "domain":
            assert all(dep.split(".")[0] in sys.stdlib_module_names for dep in imports)
        if layer in {"api", "runtime", "services", "storage"}:
            assert not any(dep == "control_plane.app" for dep in imports)
        if layer == "api":
            assert not any(dep.startswith(("control_plane.runtime", "control_plane.storage", "docker", "sqlite3")) for dep in imports)
        if layer == "runtime":
            assert not any(dep.startswith(("control_plane.storage", "control_plane.services", "control_plane.api", "sqlite3")) for dep in imports)
        if layer in {"api", "services"}:
            assert not {"docker", "sqlite3"} & {dep.split(".")[0] for dep in imports}
        if layer == "storage":
            assert not any(dep.startswith(("control_plane.runtime", "control_plane.services", "control_plane.api", "docker")) for dep in imports)
