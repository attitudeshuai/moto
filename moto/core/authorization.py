import functools
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    ClassVar,
    TypeVar,
)
from urllib.parse import urlparse

import requests
from werkzeug.exceptions import HTTPException

from moto import settings
from moto.core.auth_decision_log import (
    AuthorizationEvaluation,
    DenyCategory,
    get_auth_decision_log,
    new_request_id,
)
from moto.core.exceptions import ServiceException
from moto.utilities.utils import get_partition

if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")

T = TypeVar("T")

# IAM/S3 raise different hierarchies for the same authorization failures:
# _RESTError subclasses (HTTPException) for most services and ServiceException
# subclasses for S3.
_AUTHORIZATION_ERRORS = (HTTPException, ServiceException)


def _error_code(auth_error: Exception) -> str | None:
    """AWS error code (e.g. AccessDenied) of a raised authorization error."""
    error_type = getattr(auth_error, "error_type", None)
    if error_type:
        return error_type
    code = getattr(auth_error, "code", None)
    return code if isinstance(code, str) else None


class ActionAuthenticatorMixin:
    request_count: ClassVar[int] = 0

    PUBLIC_OPERATIONS = [
        "AWSCognitoIdentityService.GetId",
        "AWSCognitoIdentityService.GetOpenIdToken",
        "AWSCognitoIdentityProviderService.ConfirmSignUp",
        "AWSCognitoIdentityProviderService.GetUser",
        "AWSCognitoIdentityProviderService.ForgotPassword",
        "AWSCognitoIdentityProviderService.InitiateAuth",
        "AWSCognitoIdentityProviderService.SignUp",
    ]

    def _authenticate_and_authorize_action(
        self, iam_request_cls: type, resource: str = "*"
    ) -> None:
        if (
            ActionAuthenticatorMixin.request_count
            >= settings.INITIAL_NO_AUTH_ACTION_COUNT
        ):
            if (
                self.headers.get("X-Amz-Target")  # type: ignore[attr-defined]
                in ActionAuthenticatorMixin.PUBLIC_OPERATIONS
            ):
                return
            parsed_url = urlparse(self.uri)  # type: ignore[attr-defined]
            path = parsed_url.path
            if parsed_url.query:
                path += "?" + parsed_url.query
            action = self._get_action()  # type: ignore[attr-defined]
            region = getattr(self, "region", None)
            decision_log = get_auth_decision_log()
            request_id = new_request_id()

            try:
                iam_request = iam_request_cls(
                    account_id=self.current_account,  # type: ignore[attr-defined]
                    method=self.method,  # type: ignore[attr-defined]
                    path=path,
                    data=self.data,  # type: ignore[attr-defined]
                    body=self.raw_body,  # type: ignore[attr-defined]
                    headers=self.headers,  # type: ignore[attr-defined]
                    action=action,
                )
            except _AUTHORIZATION_ERRORS as auth_error:
                # Invalid access key id / security token: the request object
                # could not be built, so record a minimal deny decision.
                evaluation = AuthorizationEvaluation(
                    action=action[0] if isinstance(action, list) else str(action),
                    resource=resource,
                )
                evaluation.deny(
                    DenyCategory.INVALID_ACCESS_KEY,
                    "The provided access key id or security token is invalid",
                )
                decision_log.record(
                    request_id=request_id,
                    account_id=self.current_account,  # type: ignore[attr-defined]
                    region=region,
                    service=getattr(self, "service_name", None),
                    evaluation=evaluation,
                    error_code=_error_code(auth_error),
                )
                raise

            try:
                iam_request.check_signature()
            except _AUTHORIZATION_ERRORS as auth_error:
                decision_log.record(
                    request_id=request_id,
                    account_id=self.current_account,  # type: ignore[attr-defined]
                    region=region,
                    service=iam_request._service,
                    evaluation=iam_request.evaluation,  # type: ignore[arg-type]
                    error_code=_error_code(auth_error),
                )
                raise

            # Failure injection: force a policy-style denial for configured
            # actions/resources. Uses the same raise path as a real explicit
            # deny, but the recorded decision is marked as injected.
            injection_rule = decision_log.find_injection_rule(
                iam_request._action, resource
            )
            if injection_rule is not None:
                evaluation = AuthorizationEvaluation(
                    action=iam_request._action,
                    resource=resource,
                    principal=iam_request._access_key.arn,
                )
                evaluation.deny(
                    DenyCategory.INJECTED,
                    f"Injection rule '{injection_rule.name}' matched the request",
                    injection_rule=injection_rule,
                )
                decision_log.record(
                    request_id=request_id,
                    account_id=self.current_account,  # type: ignore[attr-defined]
                    region=region,
                    service=iam_request._service,
                    evaluation=evaluation,
                    error_code="AccessDenied",
                )
                iam_request._raise_access_denied()

            try:
                evaluation = iam_request.check_action_permitted(resource)
            except _AUTHORIZATION_ERRORS as auth_error:
                decision_log.record(
                    request_id=request_id,
                    account_id=self.current_account,  # type: ignore[attr-defined]
                    region=region,
                    service=iam_request._service,
                    evaluation=iam_request.evaluation,  # type: ignore[arg-type]
                    error_code=_error_code(auth_error),
                )
                raise
            decision_log.record(
                request_id=request_id,
                account_id=self.current_account,  # type: ignore[attr-defined]
                region=region,
                service=iam_request._service,
                evaluation=evaluation,
            )
        else:
            ActionAuthenticatorMixin.request_count += 1

    def _authenticate_and_authorize_normal_action(self, resource: str = "*") -> None:
        from moto.iam.access_control import IAMRequest

        self._authenticate_and_authorize_action(IAMRequest, resource)

    def _authenticate_and_authorize_s3_action(
        self, bucket_name: str | None = None, key_name: str | None = None
    ) -> None:
        arn = f"{bucket_name or '*'}/{key_name}" if key_name else (bucket_name or "*")
        resource = f"arn:{get_partition(self.region)}:s3:::{arn}"  # type: ignore[attr-defined]

        from moto.iam.access_control import S3IAMRequest

        self._authenticate_and_authorize_action(S3IAMRequest, resource)

    @staticmethod
    def set_initial_no_auth_action_count(
        initial_no_auth_action_count: int,
    ) -> "Callable[[Callable[P, T]], Callable[P, T]]":
        _test_server_mode_endpoint = settings.test_server_mode_endpoint()

        def decorator(function: "Callable[P, T]") -> "Callable[P, T]":
            def wrapper(*args: "P.args", **kwargs: "P.kwargs") -> T:
                if settings.TEST_SERVER_MODE:
                    response = requests.post(
                        f"{_test_server_mode_endpoint}/moto-api/reset-auth",
                        data=str(initial_no_auth_action_count).encode("utf-8"),
                    )
                    original_initial_no_auth_action_count = response.json()[
                        "PREVIOUS_INITIAL_NO_AUTH_ACTION_COUNT"
                    ]
                else:
                    original_initial_no_auth_action_count = (
                        settings.INITIAL_NO_AUTH_ACTION_COUNT
                    )
                    original_request_count = ActionAuthenticatorMixin.request_count
                    settings.INITIAL_NO_AUTH_ACTION_COUNT = initial_no_auth_action_count
                    ActionAuthenticatorMixin.request_count = 0
                try:
                    result = function(*args, **kwargs)
                finally:
                    if settings.TEST_SERVER_MODE:
                        requests.post(
                            f"{_test_server_mode_endpoint}/moto-api/reset-auth",
                            data=str(original_initial_no_auth_action_count).encode(
                                "utf-8"
                            ),
                        )
                    else:
                        ActionAuthenticatorMixin.request_count = original_request_count
                        settings.INITIAL_NO_AUTH_ACTION_COUNT = (
                            original_initial_no_auth_action_count
                        )
                return result

            functools.update_wrapper(wrapper, function)
            wrapper.__wrapped__ = function  # type: ignore[attr-defined]
            return wrapper

        return decorator


@contextmanager
def enable_iam_authentication() -> Generator[None, None, None]:
    """Make it so that all fixtures and tests run with a simulation of IAM permissions."""

    old_initial_no_auth_action_count = settings.INITIAL_NO_AUTH_ACTION_COUNT
    old_request_count = ActionAuthenticatorMixin.request_count
    settings.INITIAL_NO_AUTH_ACTION_COUNT = 0
    ActionAuthenticatorMixin.request_count = 0
    try:
        yield
    finally:
        settings.INITIAL_NO_AUTH_ACTION_COUNT = old_initial_no_auth_action_count
        ActionAuthenticatorMixin.request_count = old_request_count


@contextmanager
def disable_iam_authentication() -> Generator[None, None, None]:
    """Inverse of enable()."""

    old_initial_no_auth_action_count = settings.INITIAL_NO_AUTH_ACTION_COUNT
    old_request_count = ActionAuthenticatorMixin.request_count
    settings.INITIAL_NO_AUTH_ACTION_COUNT = float("inf")
    ActionAuthenticatorMixin.request_count = 0
    try:
        yield
    finally:
        settings.INITIAL_NO_AUTH_ACTION_COUNT = old_initial_no_auth_action_count
        ActionAuthenticatorMixin.request_count = old_request_count
