"""Validate that stored credentials still match the original authenticated entry."""

import hashlib
import hmac
import re

from control_plane.domain.errors import SandboxEndpointAuthUnavailable, SandboxMetadataConflict
from control_plane.domain.models import SandboxAccessCredential, SandboxEndpoint, SandboxLifecycleRecord
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
