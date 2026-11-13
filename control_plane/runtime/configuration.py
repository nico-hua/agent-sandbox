"""Immutable trusted backend selection and lazy construction, independent of HTTP requests."""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from control_plane.runtime.sandbox_runtime import SandboxRuntime


@dataclass(frozen=True)
class BackendConfiguration:
    """Keep selected values copied from one assembly, without reading certificates or opening clients."""

    name: str
    kubeconfig_path: str | None = field(default=None, repr=False)
    context: str | None = field(default=None, repr=False)
    namespace: str | None = field(default=None, repr=False)
    agent_image: str | None = field(default=None, repr=False)
    proxy_image: str | None = field(default=None, repr=False)
    workspace_storage_class: str | None = field(default=None, repr=False)


def _configured_value(environ: Mapping[str, str], key: str, maximum: int, *, optional=False) -> str | None:
    """Reject missing, empty or malformed trusted values without disclosing their contents."""
    if optional and key not in environ:
        return None
    value = environ.get(key)
    if (type(value) is not str or not 1 <= len(value) <= maximum or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError("invalid backend configuration")
    return value


def load_backend_configuration(environ: Mapping[str, str] | None = None) -> BackendConfiguration:
    """Snapshot only the selected backend's explicit settings; absent selection defaults to Docker."""
    source = os.environ if environ is None else environ
    name = source.get("AGENT_SANDBOX_RUNTIME", "docker")
    if type(name) is not str or name not in {"docker", "kubernetes"}:
        raise ValueError("invalid backend selection")
    if name == "docker":
        return BackendConfiguration(name)
    return BackendConfiguration(
        name=name,
        kubeconfig_path=_configured_value(source, "AGENT_SANDBOX_K8S_KUBECONFIG", 4096),
        context=_configured_value(source, "AGENT_SANDBOX_K8S_CONTEXT", 253),
        namespace=_configured_value(source, "AGENT_SANDBOX_K8S_NAMESPACE", 63),
        agent_image=_configured_value(source, "AGENT_SANDBOX_K8S_AGENT_IMAGE", 512),
        proxy_image=_configured_value(source, "AGENT_SANDBOX_K8S_PROXY_IMAGE", 512),
        workspace_storage_class=_configured_value(source, "AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", 253, optional=True),
    )


def build_runtime(configuration: BackendConfiguration, *, additional_environments=None) -> SandboxRuntime:
    """Construct the selected inert Runtime; formal Kubernetes selection always enables its proxy layout."""
    if configuration.name == "docker":
        from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
        return DockerSandboxRuntime(additional_environments=additional_environments)
    if configuration.name != "kubernetes":
        raise ValueError("invalid backend selection")
    if additional_environments is not None:
        raise ValueError("Kubernetes custom environments are not supported")
    if any(value is None for value in (
        configuration.kubeconfig_path, configuration.context, configuration.namespace,
        configuration.agent_image, configuration.proxy_image,
    )):
        raise ValueError("incomplete Kubernetes runtime configuration")
    from control_plane.core.errors import KubernetesConfigurationError
    from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
    try:
        return KubernetesSandboxRuntime(
            kubeconfig_path=configuration.kubeconfig_path, context=configuration.context,
            namespace=configuration.namespace, agent_image=configuration.agent_image,
            proxy_image=configuration.proxy_image, workspace_storage_class=configuration.workspace_storage_class,
            prepare_entry_on_create=True,
        )
    except KubernetesConfigurationError:
        raise ValueError("invalid Kubernetes runtime configuration") from None


def configured_database_path() -> str | None:
    """Read an optional trusted storage path once, without touching the filesystem or silently trimming it."""
    return _configured_value(os.environ, "AGENT_SANDBOX_DATABASE_PATH", 4096, optional=True)
