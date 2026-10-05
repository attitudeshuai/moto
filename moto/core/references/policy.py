from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from .coordinates import ResourceCoordinate
from .registry import reference_registry

Operation = Literal["delete", "replace"]


class PolicyValue(str, Enum):
    """Configurable handling policy for a referenced object."""

    DENY = "deny"
    WARN = "warn"
    CASCADE = "cascade"


# Policy storage keys. The shape encodes the scope:
#   ("global",)
#   ("service", service_name, resource_type_or_empty)
#   ("account", account_id, service_or_empty)
PolicyKey = tuple[str, ...]


@dataclass(frozen=True)
class PolicyResolution:
    """Outcome of policy resolution for one target/operation."""

    policy: PolicyValue | None  # None == passive
    source: str
    operation: Operation

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy.value if self.policy is not None else "passive",
            "source": self.source,
            "operation": self.operation,
        }


class PolicyBook:
    """Stores reference handling policies across service/account scopes.

    Resolution order (first match wins):
      1. (account + service) exact combination
      2. account-wide policy
      3. (service + resource type), then service-wide policy
      4. global default
      5. otherwise passive (no enforcement)

    Mutations share the registry lock, so policy configuration can never be
    observed interleaved with an in-progress guarded operation.
    """

    GLOBAL_KEY: PolicyKey = ("global",)

    def __init__(self) -> None:
        self._policies: dict[PolicyKey, PolicyValue] = {}

    def reset(self) -> None:
        with reference_registry.lock:
            self._policies.clear()

    # -- setters -----------------------------------------------------------

    def set_global_policy(self, policy: PolicyValue) -> None:
        with reference_registry.lock:
            self._policies[self.GLOBAL_KEY] = policy

    def set_service_policy(
        self,
        service: str,
        policy: PolicyValue,
        resource_type: str | None = None,
    ) -> None:
        with reference_registry.lock:
            self._policies[("service", service, resource_type or "")] = policy

    def set_account_policy(
        self,
        account_id: str,
        policy: PolicyValue,
        service: str | None = None,
    ) -> None:
        with reference_registry.lock:
            self._policies[("account", account_id, service or "")] = policy

    # -- getters / deletion -------------------------------------------------

    def _get(self, key: PolicyKey) -> PolicyValue | None:
        return self._policies.get(key)

    def get_global_policy(self) -> PolicyValue | None:
        with reference_registry.lock:
            return self._get(self.GLOBAL_KEY)

    def get_service_policy(
        self, service: str, resource_type: str | None = None
    ) -> PolicyValue | None:
        with reference_registry.lock:
            return self._get(("service", service, resource_type or ""))

    def get_account_policy(
        self, account_id: str, service: str | None = None
    ) -> PolicyValue | None:
        with reference_registry.lock:
            return self._get(("account", account_id, service or ""))

    def delete_global_policy(self) -> bool:
        with reference_registry.lock:
            return self._policies.pop(self.GLOBAL_KEY, None) is not None

    def delete_service_policy(
        self, service: str, resource_type: str | None = None
    ) -> bool:
        with reference_registry.lock:
            return (
                self._policies.pop(("service", service, resource_type or ""), None)
                is not None
            )

    def delete_account_policy(
        self, account_id: str, service: str | None = None
    ) -> bool:
        with reference_registry.lock:
            return (
                self._policies.pop(("account", account_id, service or ""), None)
                is not None
            )

    def list_policies(self) -> list[dict[str, Any]]:
        with reference_registry.lock:
            return [
                {"scope": list(key), "policy": policy.value}
                for key, policy in sorted(
                    self._policies.items(), key=lambda item: item[0]
                )
            ]

    # -- resolution ---------------------------------------------------------

    def resolve(
        self,
        target: ResourceCoordinate,
        operation: Operation = "delete",
    ) -> PolicyResolution:
        with reference_registry.lock:
            account_id = target.account_id or ""
            # 1. account + service combination
            combo_key: PolicyKey = ("account", account_id, target.service)
            policy = self._policies.get(combo_key)
            if policy is not None:
                return PolicyResolution(policy, "account+service", operation)

            # 2. account-wide
            account_key: PolicyKey = ("account", account_id, "")
            policy = self._policies.get(account_key)
            if policy is not None:
                return PolicyResolution(policy, "account", operation)

            # 3a. service + resource type
            if target.resource_type:
                service_type_key: PolicyKey = (
                    "service",
                    target.service,
                    target.resource_type,
                )
                policy = self._policies.get(service_type_key)
                if policy is not None:
                    return PolicyResolution(policy, "service+resource_type", operation)

            # 3b. service-wide
            service_key: PolicyKey = ("service", target.service, "")
            policy = self._policies.get(service_key)
            if policy is not None:
                return PolicyResolution(policy, "service", operation)

            # 4. global default
            policy = self._policies.get(self.GLOBAL_KEY)
            if policy is not None:
                return PolicyResolution(policy, "global", operation)

            # 5. passive
            return PolicyResolution(None, "default", operation)


policy_book = PolicyBook()
