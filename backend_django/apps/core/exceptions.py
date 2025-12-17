"""
Custom exception handling for the application.
"""
from rest_framework.views import exception_handler
from rest_framework.response import Response
from rest_framework import status
from django.core.exceptions import ValidationError as DjangoValidationError
from rest_framework.exceptions import ValidationError as DRFValidationError
import logging

logger = logging.getLogger(__name__)


def custom_exception_handler(exc, context):
    """
    Custom exception handler that provides consistent error responses.
    """
    # Call REST framework's default exception handler first
    response = exception_handler(exc, context)

    if response is not None:
        # Customize the response format
        custom_response_data = {
            'error': True,
            'status_code': response.status_code,
            'message': get_error_message(exc),
            'details': response.data if isinstance(response.data, dict) else {'detail': response.data}
        }
        response.data = custom_response_data
        return response

    # Handle Django validation errors
    if isinstance(exc, DjangoValidationError):
        data = {
            'error': True,
            'status_code': status.HTTP_400_BAD_REQUEST,
            'message': 'Validation error',
            'details': {'errors': exc.messages if hasattr(exc, 'messages') else [str(exc)]}
        }
        return Response(data, status=status.HTTP_400_BAD_REQUEST)

    # Log unhandled exceptions
    logger.exception(f"Unhandled exception: {exc}")

    # Return generic error for unhandled exceptions
    data = {
        'error': True,
        'status_code': status.HTTP_500_INTERNAL_SERVER_ERROR,
        'message': 'An unexpected error occurred',
        'details': {}
    }
    return Response(data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def get_error_message(exc):
    """Extract a human-readable message from an exception."""
    if hasattr(exc, 'detail'):
        if isinstance(exc.detail, str):
            return exc.detail
        if isinstance(exc.detail, list) and len(exc.detail) > 0:
            return str(exc.detail[0])
        if isinstance(exc.detail, dict):
            # Get first error message from dict
            for key, value in exc.detail.items():
                if isinstance(value, list) and len(value) > 0:
                    return f"{key}: {value[0]}"
                return f"{key}: {value}"
    return str(exc)


class ServiceError(Exception):
    """Base exception for service layer errors."""
    def __init__(self, message, code=None, details=None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(message)


class NotFoundError(ServiceError):
    """Resource not found error."""
    pass


class PermissionDeniedError(ServiceError):
    """Permission denied error."""
    pass


class ConfigurationError(ServiceError):
    """Configuration error."""
    pass


class BigQueryError(ServiceError):
    """BigQuery related error."""
    pass
