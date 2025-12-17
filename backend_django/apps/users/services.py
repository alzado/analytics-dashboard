"""
User authentication services.
"""
import logging
from datetime import datetime
from django.conf import settings
from django.utils import timezone
from google.oauth2 import id_token
from google.auth.transport import requests
from rest_framework.exceptions import AuthenticationFailed

from .models import User
from .authentication import create_access_token, create_refresh_token, decode_token

logger = logging.getLogger(__name__)


class AuthService:
    """Service for handling authentication."""

    @staticmethod
    def verify_google_token(token: str) -> dict:
        """
        Verify a Google ID token and return user info.

        Args:
            token: Google ID token from frontend

        Returns:
            Dictionary with user info (google_id, email, name, picture)

        Raises:
            AuthenticationFailed: If token is invalid
        """
        try:
            # Verify the token with Google
            idinfo = id_token.verify_oauth2_token(
                token,
                requests.Request(),
                settings.GOOGLE_CLIENT_ID
            )

            # Verify issuer
            if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
                raise AuthenticationFailed('Invalid token issuer')

            return {
                'google_id': idinfo['sub'],
                'email': idinfo['email'],
                'name': idinfo.get('name', idinfo['email'].split('@')[0]),
                'picture': idinfo.get('picture')
            }

        except ValueError as e:
            logger.error(f"Google token verification failed: {e}")
            raise AuthenticationFailed('Invalid Google token')

    @staticmethod
    def get_or_create_user(google_info: dict) -> User:
        """
        Get or create a user from Google OAuth info.

        Args:
            google_info: Dictionary with google_id, email, name, picture

        Returns:
            User instance
        """
        try:
            # Try to find existing user by Google ID
            user = User.objects.get(google_id=google_info['google_id'])
            # Update user info
            user.name = google_info['name']
            user.avatar_url = google_info.get('picture')
            user.last_login_at = timezone.now()
            user.save()
            return user
        except User.DoesNotExist:
            pass

        # Try to find by email (for users who existed before OAuth)
        try:
            user = User.objects.get(email=google_info['email'])
            # Link Google ID
            user.google_id = google_info['google_id']
            user.name = google_info['name']
            user.avatar_url = google_info.get('picture')
            user.last_login_at = timezone.now()
            user.save()
            return user
        except User.DoesNotExist:
            pass

        # Create new user
        user = User.objects.create_user(
            email=google_info['email'],
            name=google_info['name'],
            google_id=google_info['google_id'],
            avatar_url=google_info.get('picture'),
            last_login_at=timezone.now()
        )
        return user

    @staticmethod
    def authenticate_google(token: str) -> dict:
        """
        Authenticate a user with Google OAuth.

        Args:
            token: Google ID token

        Returns:
            Dictionary with access_token, refresh_token, and user info
        """
        # Verify Google token
        google_info = AuthService.verify_google_token(token)

        # Get or create user
        user = AuthService.get_or_create_user(google_info)

        # Generate JWT tokens
        access_token = create_access_token(user.id)
        refresh_token = create_refresh_token(user.id)

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'Bearer',
            'user': user
        }

    @staticmethod
    def refresh_access_token(refresh_token: str) -> dict:
        """
        Refresh an access token using a refresh token.

        Args:
            refresh_token: JWT refresh token

        Returns:
            Dictionary with new access_token and refresh_token

        Raises:
            AuthenticationFailed: If refresh token is invalid
        """
        payload = decode_token(refresh_token)

        if payload.get('type') != 'refresh':
            raise AuthenticationFailed('Invalid token type')

        user_id = payload.get('user_id')
        if not user_id:
            raise AuthenticationFailed('Invalid token payload')

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            raise AuthenticationFailed('User not found')

        if not user.is_active:
            raise AuthenticationFailed('User is inactive')

        # Generate new tokens
        new_access_token = create_access_token(user.id)
        new_refresh_token = create_refresh_token(user.id)

        return {
            'access_token': new_access_token,
            'refresh_token': new_refresh_token,
            'token_type': 'Bearer',
            'user': user
        }
