"""
JWT Authentication for Django REST Framework.
"""
import jwt
from datetime import datetime, timedelta
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from .models import User


class JWTAuthentication(BaseAuthentication):
    """JWT-based authentication."""

    def authenticate(self, request):
        """Authenticate the request and return a tuple of (user, token)."""
        auth_header = request.headers.get('Authorization')

        if not auth_header:
            return None

        if not auth_header.startswith('Bearer '):
            return None

        token = auth_header[7:]

        try:
            payload = jwt.decode(
                token,
                settings.JWT_SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM]
            )

            user_id = payload.get('user_id')
            token_type = payload.get('type', 'access')

            if not user_id:
                raise AuthenticationFailed('Invalid token payload')

            if token_type != 'access':
                raise AuthenticationFailed('Invalid token type')

            try:
                user = User.objects.get(id=user_id)
            except User.DoesNotExist:
                raise AuthenticationFailed('User not found')

            if not user.is_active:
                raise AuthenticationFailed('User is inactive')

            return (user, token)

        except jwt.ExpiredSignatureError:
            raise AuthenticationFailed('Token has expired')
        except jwt.InvalidTokenError as e:
            raise AuthenticationFailed(f'Invalid token: {str(e)}')

    def authenticate_header(self, request):
        """Return a string to be used as the value of the WWW-Authenticate header."""
        return 'Bearer'


def create_access_token(user_id: str) -> str:
    """Create an access token for a user."""
    payload = {
        'user_id': str(user_id),
        'type': 'access',
        'exp': datetime.utcnow() + timedelta(hours=settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS),
        'iat': datetime.utcnow()
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(user_id: str) -> str:
    """Create a refresh token for a user."""
    payload = {
        'user_id': str(user_id),
        'type': 'refresh',
        'exp': datetime.utcnow() + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS),
        'iat': datetime.utcnow()
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """Decode a JWT token and return the payload."""
    try:
        return jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
    except jwt.ExpiredSignatureError:
        raise AuthenticationFailed('Token has expired')
    except jwt.InvalidTokenError as e:
        raise AuthenticationFailed(f'Invalid token: {str(e)}')
