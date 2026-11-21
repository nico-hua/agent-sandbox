"""Authorize local entry delivery against the original sandbox transaction."""

import hashlib
import hmac
import re
from dataclasses import replace

from control_plane.domain.endpoint import (
    SandboxAccessCredential,
    SandboxEndpoint,
    SandboxServiceEndpoint,
    _validate_service_port,
)
from control_plane.domain.errors import (
    SandboxEndpointAuthRequired,
    SandboxEndpointAuthUnavailable,
    SandboxEndpointUnavailable,
    SandboxMetadataConflict,
    SandboxServiceUnsupported,
)
from control_plane.domain.sandbox import SandboxLifecycleRecord
from control_plane.runtime.base_runtime import SandboxRuntime
from control_plane.services.sandbox_queries import (
    SandboxQueries,
    read_lifecycle_record,
    require_sandbox_id,
)
from control_plane.storage.base import LifecycleStore, LifecycleStoreConflict


async def verified_endpoint_credential(
    store: LifecycleStore, record: SandboxLifecycleRecord, endpoint: SandboxEndpoint,
) -> SandboxAccessCredential:
    """Verify a persisted secret against the actual proxy without regenerating or adopting it."""
    if endpoint.auth_required is not True or endpoint.auth_version != "1":
        raise SandboxEndpointAuthUnavailable()
    if (
        endpoint.sandbox_id != record.sandbox_id
        or endpoint.transaction_id != record.transaction_id
        or endpoint.created_at != record.created_at
    ):
        raise SandboxMetadataConflict(record.sandbox_id)
    try:
        credential = await store.get_credential(record.sandbox_id, record.transaction_id)
    except LifecycleStoreConflict as error:
        raise SandboxMetadataConflict(record.sandbox_id) from error
    except Exception as error:
        raise SandboxEndpointAuthUnavailable() from error
    if credential is None:
        raise SandboxEndpointAuthUnavailable()
    if credential.sandbox_id != record.sandbox_id or credential.transaction_id != record.transaction_id:
        raise SandboxMetadataConflict(record.sandbox_id)
    digest = endpoint.credential_digest
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise SandboxEndpointAuthUnavailable()
    expected = hashlib.sha256(credential.access_token.encode("ascii")).hexdigest()
    if not hmac.compare_digest(expected, digest):
        raise SandboxEndpointAuthUnavailable()
    return credential


class EndpointAccess:
    """Deliver only local endpoints with verified runtime and persisted identity."""

    def __init__(self, runtime: SandboxRuntime, store: LifecycleStore, queries: SandboxQueries) -> None:
        """Use the same read policy as other sandbox queries before verifying credentials."""
        self._runtime = runtime
        self._store = store
        self._queries = queries

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Deliver only a protected entry whose resource and stored credential identities match."""
        require_sandbox_id(sandbox_id)
        endpoint = await self._runtime.get_endpoint(sandbox_id)
        if endpoint.sandbox_id != sandbox_id:
            raise SandboxEndpointUnavailable()
        if endpoint.auth_required is False and endpoint.auth_version is None:
            raise SandboxEndpointAuthRequired()
        observed = await self._queries.get(sandbox_id)
        record = await read_lifecycle_record(self._store, sandbox_id)
        if record is None:
            raise SandboxEndpointAuthUnavailable()
        if (
            observed.transaction_id != record.transaction_id
            or observed.created_at != record.created_at
            or observed.workspace_id != record.workspace_id
        ):
            raise SandboxMetadataConflict(sandbox_id)
        credential = await verified_endpoint_credential(self._store, record, endpoint)
        return replace(endpoint, access_token=credential.access_token)

    async def get_service_endpoint(self, sandbox_id: str, port: int) -> SandboxServiceEndpoint:
        """Reuse protected entry identity and credentials without contacting the application."""
        _validate_service_port(port)
        try:
            endpoint = await self.get_endpoint(sandbox_id)
        except SandboxEndpointAuthRequired:
            raise SandboxServiceUnsupported() from None
        if endpoint.services_version != "1":
            raise SandboxServiceUnsupported()
        try:
            return SandboxServiceEndpoint(
                sandbox_id, port, endpoint.url + f"/services/{port}/", True, endpoint.access_token,
            )
        except (ValueError, TypeError) as error:
            raise SandboxEndpointUnavailable() from error
