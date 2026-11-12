"""Bounded, read-only integrity observations, distinct from readiness and recovery."""

import base64
import binascii
import json
import re
from types import SimpleNamespace
from uuid import UUID

from kubernetes import client

from control_plane.core.errors import SandboxQueryFailed, SandboxDeleteConflict, KubernetesConfigurationError
from control_plane.core.models import SandboxResourceInspection
from control_plane.runtime.kubernetes.agent_service import verify_clusterip_service
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, PROXY_LAYOUT_ANNOTATION, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION,
    PROXY_IMAGE_ANNOTATION, PRIVATE_WORKSPACE_VERSION, PRIVATE_WORKSPACE_UID, PRIVATE_STORAGE_CLASS,
    NETWORK_LAYOUT_LABEL, NETWORK_POLICY_UID_ANNOTATION, AGENT_SERVICE_VERSION_ANNOTATION,
    AGENT_SERVICE_UID_ANNOTATION, lifecycle_annotations, resource_ownership, primary_pod_labels,
    sandbox_pod_selector,
)
from control_plane.runtime.kubernetes.proxy_resources import (
    ProxyResources, _RESOURCE_GROUPS, _ROLES, _MARKERS, _LAYOUT_KEYS, _record, _token_digest, _spec_matches,
)
from control_plane.runtime.kubernetes.proxy_spec import TOKEN_KEY, proxy_secret_name, proxy_network_policy_specs
from control_plane.runtime.kubernetes.private_workspace import verify_private_workspace_configuration, _dns_name
from control_plane.runtime.kubernetes.pod_spec import _validate_backend_configuration

_ORDER = ("agent", "policy", "service", "secret", "proxy", "ingress", "isolation", "proxy_service", "workspace")


class InspectionConflict(SandboxQueryFailed):
    """Keep positive conflicting evidence through worker client closure without SDK diagnostics."""


class InspectionChanged(SandboxQueryFailed):
    """Report a positively observed replacement or configuration change during this scan."""


class InspectionUnavailable(Exception):
    """Reject an incomplete backend observation without interpreting it as resource absence."""


def _signature(key, resource):
    """Snapshot identity/configuration only; normal Pod/PVC status updates do not invalidate integrity."""
    metadata = resource.metadata
    if key == "secret":
        # Never serialize credential contents into an observation, diagnostic or
        # comparison string. Secrets have no readiness status, so RV detects an
        # intervening data/metadata mutation without retaining the submitted data.
        spec = (resource.type, resource.immutable, metadata.resource_version)
    else:
        spec = json.dumps(resource.spec.to_dict(), sort_keys=True, separators=(",", ":"))
    return (metadata.name, metadata.namespace, metadata.uid,
            tuple(sorted((k, v) for k, v in (metadata.labels or {}).items() if k.startswith(LABEL_PREFIX))),
            tuple(sorted((k, v) for k, v in (metadata.annotations or {}).items() if k.startswith(LABEL_PREFIX))),
            metadata.deletion_timestamp, spec)


class ResourceInspection:
    """Borrow one worker client and the existing closed resource map without performing any writes."""

    def __init__(self, api, sandbox_id, namespace, transaction_id):
        """Share deterministic names, role limits and one finite request budget with existing resource discovery."""
        self.manager = ProxyResources(api, sandbox_id, namespace, transaction_id)
        self.api, self.sandbox_id, self.namespace, self.transaction_id = api, sandbox_id, namespace, transaction_id

    def _namespace(self):
        """Require readable namespace existence before any missing-object conclusion."""
        namespace = self.api.read_namespace(self.namespace, _request_timeout=self.manager._timeout())
        if getattr(getattr(namespace, "metadata", None), "name", None) != self.namespace:
            raise InspectionUnavailable()

    def _identity(self, key, resource):
        """Check strict ownership/known markers without requiring a completed publication or readiness."""
        metadata = resource.metadata
        _, transaction = resource_ownership(metadata, self.sandbox_id, self.namespace,
            name=self.manager.names[key], role=_ROLES[key], check_proxy_layout=False)
        version = metadata.resource_version
        if (transaction != self.transaction_id or type(version) is not str or not version
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version)
            or any(k.startswith(LABEL_PREFIX) and k[len(LABEL_PREFIX):] not in _MARKERS
                   for k in (*metadata.labels, *(metadata.annotations or {})))
            or any(k in metadata.labels for k in (PROXY_LAYOUT_ANNOTATION, PROXY_RECORD_ANNOTATION,
                    PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION, PRIVATE_WORKSPACE_VERSION,
                    PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID))):
            raise InspectionConflict()

    def _scan(self, previous=None):
        """Cross-check bounded labels and every exact name, distinguishing positive changes from static conflicts."""
        self._namespace()
        found = {}
        for kind, keys in _RESOURCE_GROUPS:
            facade = self.api.networking if kind == "network_policy" else self.api
            page = getattr(facade, "list_namespaced_" + kind)(self.namespace,
                label_selector=sandbox_pod_selector(self.sandbox_id), limit=len(keys) + 1,
                watch=False, _request_timeout=self.manager._timeout())
            if (not isinstance(page.items, list) or page.metadata is None
                or page.metadata._continue not in (None, "")
                or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)):
                raise InspectionUnavailable()
            if len(page.items) > len(keys):
                raise InspectionConflict()
            listed = {}
            for resource in page.items:
                if previous is not None:
                    for key in keys:
                        if (key in previous and resource.metadata.name == previous[key].metadata.name
                            and _signature(key, previous[key]) != _signature(key, resource)):
                            raise InspectionChanged()
                role = (resource.metadata.labels or {}).get(LABEL_PREFIX + "resource")
                matches = [key for key in keys if _ROLES[key] == role]
                if len(matches) != 1 or matches[0] in listed:
                    raise InspectionConflict()
                key = matches[0]
                if previous is not None and (key not in previous or _signature(key, previous[key]) != _signature(key, resource)):
                    raise InspectionChanged()
                self._identity(key, resource)
                listed[key] = resource
            for key in keys:
                resource = self.manager._read(key)
                if resource is None:
                    if key in listed:
                        self._namespace()
                        raise InspectionChanged()
                    continue
                if key in listed and _signature(key, listed[key]) != _signature(key, resource):
                    raise InspectionChanged()
                self._identity(key, resource)
                if key not in listed:
                    raise InspectionChanged()
                found[key] = resource
        if previous is not None and set(previous) != set(found):
            raise InspectionChanged()
        return found

    def _publication(self, found):
        """Merge only published facts, retaining incompleteness instead of fabricating missing credentials or bindings."""
        incomplete, bindings, shared = False, {}, {}
        versions = {r.metadata.annotations[PROXY_LAYOUT_ANNOTATION] for r in found.values()
                    if PROXY_LAYOUT_ANNOTATION in (r.metadata.annotations or {})}
        if any(v not in _LAYOUT_KEYS for v in versions) or len(versions) > 1:
            raise InspectionConflict()
        version = next(iter(versions), None)
        required = _LAYOUT_KEYS.get(version)
        for key, resource in found.items():
            annotations = resource.metadata.annotations or {}
            # Independently present timestamps must agree even if another
            # resource has not published both fields yet.
            for name in ("created-at", "expires-at"):
                field = LABEL_PREFIX + name
                if field not in annotations:
                    incomplete = True
                    continue
                value = annotations[field]
                parsed = lifecycle_annotations({LABEL_PREFIX + "created-at": value, LABEL_PREFIX + "expires-at": value})[0]
                if field in shared and shared[field] != parsed:
                    raise InspectionConflict()
                shared[field] = parsed
            for field in (PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION, PRIVATE_WORKSPACE_VERSION,
                          PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID):
                if field not in annotations:
                    if version is not None and (field in {PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION} or version == "3"):
                        incomplete = True
                    continue
                value = annotations[field]
                if field == PROXY_DIGEST_ANNOTATION and (type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None):
                    raise InspectionConflict()
                if field == PROXY_IMAGE_ANNOTATION:
                    _validate_backend_configuration(self.namespace, value)
                if field == PRIVATE_WORKSPACE_VERSION and value != "1":
                    raise InspectionConflict()
                if field == PRIVATE_STORAGE_CLASS and not _dns_name(value):
                    raise InspectionConflict()
                if field == PRIVATE_WORKSPACE_UID and (type(value) is not str or str(UUID(value)) != value):
                    raise InspectionConflict()
                if field in shared and shared[field] != value:
                    raise InspectionConflict()
                shared[field] = value
            raw = annotations.get(PROXY_RECORD_ANNOTATION)
            receipt = None
            if raw is not None:
                # An absent version never hides damaged/unknown receipt fields.
                # The union is used only for validation, never to select a layout.
                if type(raw) is str and len(raw) <= 1024 and raw.strip().startswith("{") and json.loads(raw) == {}:
                    receipt, incomplete = {}, True
                else:
                    receipt = _record(SimpleNamespace(annotations={
                        PROXY_LAYOUT_ANNOTATION: version or "3", PROXY_RECORD_ANNOTATION: raw}))
            if version is not None:
                if annotations.get(PROXY_LAYOUT_ANNOTATION) != version:
                    incomplete = True
                if raw is None:
                    incomplete = True
                    continue
                if key not in receipt:
                    incomplete = True
                elif receipt[key] != resource.metadata.uid:
                    raise InspectionConflict()
                for name, uid in receipt.items():
                    if name in bindings and bindings[name] != uid:
                        raise InspectionConflict()
                    bindings[name] = uid
                if set(receipt) != required:
                    incomplete = True
            elif raw is not None or PROXY_IMAGE_ANNOTATION in annotations or PROXY_DIGEST_ANNOTATION in annotations:
                incomplete = True
        if (LABEL_PREFIX + "created-at" in shared and LABEL_PREFIX + "expires-at" in shared
            and shared[LABEL_PREFIX + "expires-at"] < shared[LABEL_PREFIX + "created-at"]):
            raise InspectionConflict()
        if required is not None:
            if not set(found) <= required:
                raise InspectionConflict()
            for key, resource in found.items():
                if key not in bindings:
                    incomplete = True
                elif bindings[key] != resource.metadata.uid:
                    raise InspectionConflict()
            if PRIVATE_WORKSPACE_UID in shared and bindings.get("workspace", shared[PRIVATE_WORKSPACE_UID]) != shared[PRIVATE_WORKSPACE_UID]:
                raise InspectionConflict()
        return version, required, bindings, shared, incomplete

    def _links(self, found, shared):
        """Reject independently verifiable routing/ownership contradictions even in incomplete layouts."""
        labels = primary_pod_labels(self.sandbox_id, self.transaction_id)
        agent = found.get("agent")
        if agent is not None:
            spec = agent.spec
            if (not isinstance(spec.containers, list) or len(spec.containers) != 1 or spec.containers[0].name != "agent"
                or spec.init_containers not in (None, []) or spec.ephemeral_containers not in (None, [])):
                raise InspectionConflict()
            if (any(getattr(spec, key) not in (None, False) for key in ("host_network", "host_pid", "host_ipc"))
                or spec.automount_service_account_token is True or spec.enable_service_links is True):
                raise InspectionConflict()
            identity = spec.security_context
            if identity is not None and (
                identity.run_as_non_root is False or identity.run_as_user not in (None, 1000)
                or identity.run_as_group not in (None, 1000) or identity.fs_group not in (None, 1000)
                or (identity.seccomp_profile is not None and identity.seccomp_profile.type != "RuntimeDefault")
            ):
                raise InspectionConflict()
            security = spec.containers[0].security_context
            if security is not None and (
                security.privileged is True or security.allow_privilege_escalation is True
                or security.read_only_root_filesystem is False
                or security.run_as_non_root is False or security.run_as_user not in (None, 1000)
                or security.run_as_group not in (None, 1000)
                or (security.seccomp_profile is not None and security.seccomp_profile.type != "RuntimeDefault")
                or (security.capabilities is not None and (security.capabilities.add not in (None, [])
                    or security.capabilities.drop not in (None, ["ALL"])))
            ):
                raise InspectionConflict()
            annotations = agent.metadata.annotations or {}
            if annotations.get(PROXY_LAYOUT_ANNOTATION) != "3" or all(
                key in annotations for key in (PRIVATE_WORKSPACE_VERSION, PRIVATE_STORAGE_CLASS)):
                from control_plane.runtime.kubernetes.pod_delete import verify_workspace_volumes
                verify_workspace_volumes(agent)
            else:
                # Missing publication is not permission to accept a positively
                # wrong claim. Preserve partial classification without inventing
                # the missing StorageClass or layout facts.
                from control_plane.runtime.kubernetes.private_workspace import private_workspace_name
                for volume in spec.volumes or []:
                    source = volume.persistent_volume_claim
                    if source is not None and source.claim_name != private_workspace_name(self.sandbox_id):
                        raise InspectionConflict()
            for field, key in ((NETWORK_POLICY_UID_ANNOTATION, "policy"), (AGENT_SERVICE_UID_ANNOTATION, "service")):
                uid = (agent.metadata.annotations or {}).get(field)
                if uid is not None and key in found and uid != found[key].metadata.uid:
                    raise InspectionConflict()
        if "policy" in found:
            spec = found["policy"].spec
            if (spec.pod_selector.match_labels != labels or spec.pod_selector.match_expressions not in (None, [])
                or spec.policy_types is None or len(spec.policy_types) != 2 or set(spec.policy_types) != {"Ingress", "Egress"}
                or spec.ingress not in (None, []) or spec.egress not in (None, [])):
                raise InspectionConflict()
        for key, role in (("service", "container"), ("proxy_service", "proxy")):
            if key in found:
                verify_clusterip_service(found[key].spec, {**labels, LABEL_PREFIX + "resource": role})
        for key, expected in zip(("ingress", "isolation"), proxy_network_policy_specs(self.sandbox_id, self.transaction_id)):
            if key in found and not _spec_matches(expected.to_dict(), found[key].spec.to_dict()):
                raise InspectionConflict()
        if "secret" in found:
            secret = found["secret"]
            if (secret.type != "Opaque" or secret.immutable is not True or secret.string_data not in (None, {})
                or not isinstance(secret.data, dict) or set(secret.data) != {TOKEN_KEY}):
                raise InspectionConflict()
            try:
                token = base64.b64decode(secret.data[TOKEN_KEY], validate=True).decode("ascii")
                digest = _token_digest(token)
                if (base64.b64encode(token.encode()).decode() != secret.data[TOKEN_KEY]
                    or (PROXY_DIGEST_ANNOTATION in shared and digest != shared[PROXY_DIGEST_ANNOTATION])):
                    raise InspectionConflict()
            except (ValueError, TypeError, UnicodeError, binascii.Error):
                raise InspectionConflict() from None
        if "proxy" in found:
            containers = found["proxy"].spec.containers
            if not isinstance(containers, list) or len(containers) != 1 or containers[0].name != "proxy":
                raise InspectionConflict()
            env = containers[0].env or []
            auth = [item for item in env if item.name == "SANDBOX_PROXY_AUTH_VERSION"]
            upstream = [item for item in env if item.name == "SANDBOX_AGENT_UPSTREAM_IPV4"]
            if (len(auth) != 1 or auth[0].value != "1" or auth[0].value_from is not None
                or len(upstream) != 1 or upstream[0].value_from is not None
                or ("service" in found and upstream[0].value != found["service"].spec.cluster_ip)):
                raise InspectionConflict()
            credentials = [env for env in containers[0].env or [] if env.name == "SANDBOX_ACCESS_TOKEN"]
            if (len(credentials) != 1 or credentials[0].value is not None or credentials[0].value_from is None
                or credentials[0].value_from.secret_key_ref is None
                or credentials[0].value_from.secret_key_ref.name != proxy_secret_name(self.sandbox_id)
                or credentials[0].value_from.secret_key_ref.key != TOKEN_KEY):
                raise InspectionConflict()
        if "workspace" in found:
            pvc = found["workspace"]
            verify_private_workspace_configuration(pvc, shared.get(PRIVATE_STORAGE_CLASS, pvc.spec.storage_class_name))
            if PRIVATE_WORKSPACE_UID in shared and shared[PRIVATE_WORKSPACE_UID] != pvc.metadata.uid:
                raise InspectionConflict()

    def _classify(self, found):
        """Describe persisted layout completeness without querying status, Agent HTTP or current Runtime settings."""
        if not found:
            return SandboxResourceInspection(self.sandbox_id, "not_found", "kubernetes_resources_not_found")
        version, required, bindings, shared, incomplete = self._publication(found)
        self._links(found, shared)
        if version is None:
            proxy_facts = set(found) - {"agent", "policy", "service"}
            if proxy_facts or any(k in shared for k in (PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION)):
                incomplete, required = True, set(found)
            else:
                required = {"agent"}
                if "policy" in found or "service" in found or any(NETWORK_LAYOUT_LABEL in (r.metadata.labels or {}) for r in found.values()):
                    required.add("policy")
                if "service" in found or any(AGENT_SERVICE_VERSION_ANNOTATION in (r.metadata.annotations or {}) for r in found.values()):
                    required.add("service")
                if "agent" in found:
                    from control_plane.runtime.kubernetes.pod_delete import _deletion_identity
                    _deletion_identity(found["agent"], self.sandbox_id, self.namespace, self.transaction_id)
                    for key, field in (("policy", NETWORK_POLICY_UID_ANNOTATION), ("service", AGENT_SERVICE_UID_ANNOTATION)):
                        if key in required and field not in (found["agent"].metadata.annotations or {}):
                            incomplete = True
        if required is not None and not set(found) <= required:
            raise InspectionConflict()
        missing = set(required) - set(found)
        deleting = any(r.metadata.deletion_timestamp is not None for r in found.values())
        if not incomplete and not missing and version is not None and not deleting:
            self.manager._verify_configuration(found, bindings, found["agent"])
        if incomplete:
            classification, reason = "partial", "kubernetes_identity_incomplete"
        elif deleting:
            classification, reason = "partial", "kubernetes_resources_deleting"
        elif version == "3" and set(found) == {"workspace"}:
            classification, reason = "partial", "kubernetes_private_workspace_retained"
        elif missing:
            classification, reason = "partial", "kubernetes_resources_partial"
        else:
            classification, reason = "all_present", "kubernetes_resources_complete"
        return SandboxResourceInspection(self.sandbox_id, classification, reason,
            tuple(_ROLES[k] for k in _ORDER if k in found), tuple(_ROLES[k] for k in _ORDER if k in missing))

    def run(self):
        """Require two agreeing bounded observations; this remains an observation, never an atomic snapshot."""
        try:
            first = self._scan()
            result = self._classify(first)
            self._scan(previous=first)
            return result
        except (InspectionConflict, InspectionChanged, InspectionUnavailable):
            raise
        except (SandboxQueryFailed, SandboxDeleteConflict, KubernetesConfigurationError,
                AttributeError, KeyError, TypeError, ValueError):
            raise InspectionConflict() from None
