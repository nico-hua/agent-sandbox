"""Validate trusted Docker image references and resolve immutable environment images."""

import re
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from docker.errors import ImageNotFound

from control_plane.domain.errors import (
    SandboxEnvironmentNotRegistered,
    SandboxImageUnavailable,
    WorkspaceUnavailable,
)
from control_plane.domain.sandbox import (
    BUILTIN_ENVIRONMENTS,
    MAX_CUSTOM_ENVIRONMENTS,
    _validate_environment,
)

SANDBOX_IMAGE = "agent-sandbox-basic:dev"


ENVIRONMENT_IMAGES = MappingProxyType({
    name: SANDBOX_IMAGE if name == "basic" else f"agent-sandbox-{name}:dev"
    for name in BUILTIN_ENVIRONMENTS
})


PROXY_IMAGE = "agent-sandbox-proxy:dev"


EGRESS_IMAGE = "agent-sandbox-egress:dev"


def _validate_image_reference(value: str) -> None:
    """Check a bounded ASCII Docker reference subset locally, without resolving registries."""
    if type(value) is not str or not 1 <= len(value) <= 512 or not value.isascii():
        raise ValueError("invalid environment image reference")
    reference, separator, digest = value.partition("@")
    if separator and re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ValueError("invalid environment image digest")
    last_component = reference.rsplit("/", 1)[-1]
    if ":" in last_component:
        reference, tag = reference.rsplit(":", 1)
        if re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag) is None:
            raise ValueError("invalid environment image tag")
    if not 1 <= len(reference) <= 255:
        raise ValueError("invalid environment image name")
    components = reference.split("/")
    first = components[0]
    if len(components) > 1 and ("." in first or ":" in first or first == "localhost"):
        hostname, separator, port = first.partition(":")
        label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
        if len(hostname) > 253 or re.fullmatch(rf"{label}(?:\.{label})*", hostname) is None:
            raise ValueError("invalid environment image registry")
        if separator and (re.fullmatch(r"[0-9]{1,5}", port) is None or not 1 <= int(port) <= 65535):
            raise ValueError("invalid environment image registry port")
        components = components[1:]
    # This intentionally excludes URL schemes, credentials and IPv6 registry literals.
    component = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
    if any(re.fullmatch(component, part) is None for part in components):
        raise ValueError("invalid environment image repository")


def build_environment_images(additional: Mapping[str, str] | None = None) -> Mapping[str, str]:
    """Validate operator additions and freeze an independent map without overriding builtin images."""
    images = dict(ENVIRONMENT_IMAGES)
    if additional is not None:
        if not isinstance(additional, Mapping) or len(additional) > MAX_CUSTOM_ENVIRONMENTS:
            raise ValueError("invalid additional environment mapping")
        for name, image in additional.items():
            _validate_environment(name)
            if name in ENVIRONMENT_IMAGES:
                raise ValueError("builtin environment images cannot be overridden")
            _validate_image_reference(image)
            images[name] = image
    return MappingProxyType(images)


def _validate_image_mounts(image: Any) -> None:
    """Reject inherited anonymous volumes that the creation ledger cannot own or clean up."""
    attributes = getattr(image, "attrs", None)
    config = attributes.get("Config") if isinstance(attributes, dict) else None
    if not isinstance(config, dict):
        raise SandboxImageUnavailable()
    volumes = config.get("Volumes")
    # Docker merges image VOLUME declarations with requested mounts. Only the
    # explicitly bound workspace is safe; extra destinations create untracked volumes.
    if volumes is not None and (
        not isinstance(volumes, dict) or set(volumes) - {"/workspace"}
    ):
        raise SandboxImageUnavailable()


def resolve_environment_image_sync(
    factory: Callable[[], Any], environment: str, images: Mapping[str, str],
) -> str:
    """Preflight only the selected local Agent image and pin its immutable ID."""
    if environment not in images:
        raise SandboxEnvironmentNotRegistered()
    client = None
    completed = False
    try:
        client = factory()
        image = client.images.get(images[environment])
        _validate_image_mounts(image)
        if not isinstance(image.id, str) or not image.id:
            raise SandboxImageUnavailable()
        completed = True
        return image.id
    except (SandboxEnvironmentNotRegistered, SandboxImageUnavailable):
        raise
    except ImageNotFound as error:
        raise SandboxImageUnavailable() from error
    except Exception as error:
        raise WorkspaceUnavailable() from error
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as error:
                if completed:
                    raise WorkspaceUnavailable() from error
