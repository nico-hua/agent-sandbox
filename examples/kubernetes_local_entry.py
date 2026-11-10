"""Open a temporary WSL loopback session for an existing ready eight-resource Kubernetes sandbox."""

import argparse
import asyncio
from pathlib import Path

from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry


async def run(args):
    """Hold only the local forward until Ctrl+C; never print, retrieve or send the instance credential."""
    backend = KubernetesSandboxRuntime(kubeconfig_path=args.kubeconfig, context=args.context, namespace=args.namespace)
    async with KubernetesLocalEntry(backend, args.sandbox_id) as entry:
        print(f"WSL local entry: {entry.url}", flush=True)
        print("Use the existing instance Access Token; Ctrl+C closes only this local forward.", flush=True)
        while entry.is_active:
            await asyncio.sleep(0.5)
        raise RuntimeError("local entry session is no longer available")


def main():
    """Require an explicit kubeconfig/context/namespace and an existing public sandbox identity."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("sandbox_id")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
