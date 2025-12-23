"""
GCP OAuth service for BigQuery access.

This service handles the OAuth flow to grant users BigQuery access using their
own Google credentials instead of a shared service account.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
from urllib.parse import urlencode

import requests
from django.conf import settings
from django.utils import timezone
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from rest_framework.exceptions import AuthenticationFailed

from .models import User

logger = logging.getLogger(__name__)

# Google OAuth endpoints
GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'


class GCPOAuthService:
    """Service for handling GCP OAuth for BigQuery access."""

    # Scopes needed for BigQuery access
    BIGQUERY_SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/bigquery.readonly',
        'https://www.googleapis.com/auth/cloud-platform.read-only',  # For listing projects
    ]

    @staticmethod
    def get_authorization_url(state: str = None, redirect_uri: str = None) -> str:
        """
        Generate Google OAuth authorization URL for BigQuery access.

        Args:
            state: Optional state parameter for CSRF protection
            redirect_uri: Override the default redirect URI

        Returns:
            Authorization URL to redirect user to
        """
        params = {
            'client_id': settings.GOOGLE_CLIENT_ID,
            'redirect_uri': redirect_uri or settings.GOOGLE_REDIRECT_URI,
            'response_type': 'code',
            'scope': ' '.join(GCPOAuthService.BIGQUERY_SCOPES),
            'access_type': 'offline',  # Get refresh token
            'prompt': 'consent',  # Always show consent screen to get refresh token
            'include_granted_scopes': 'true',
        }

        if state:
            params['state'] = state

        return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"

    @staticmethod
    def exchange_code_for_tokens(
        code: str,
        redirect_uri: str = None
    ) -> dict:
        """
        Exchange authorization code for access and refresh tokens.

        Args:
            code: Authorization code from Google OAuth callback
            redirect_uri: The redirect URI used in the authorization request

        Returns:
            Dict with access_token, refresh_token, expires_in, scope

        Raises:
            AuthenticationFailed: If token exchange fails
        """
        data = {
            'client_id': settings.GOOGLE_CLIENT_ID,
            'client_secret': settings.GOOGLE_CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri or settings.GOOGLE_REDIRECT_URI,
        }

        try:
            response = requests.post(GOOGLE_TOKEN_URL, data=data)
            response.raise_for_status()
            token_data = response.json()

            if 'error' in token_data:
                logger.error(f"Token exchange error: {token_data}")
                raise AuthenticationFailed(
                    f"Failed to exchange code: {token_data.get('error_description', token_data['error'])}"
                )

            return {
                'access_token': token_data['access_token'],
                'refresh_token': token_data.get('refresh_token'),
                'expires_in': token_data.get('expires_in', 3600),
                'scope': token_data.get('scope', '').split(),
                'token_type': token_data.get('token_type', 'Bearer'),
            }

        except requests.RequestException as e:
            logger.error(f"Token exchange request failed: {e}")
            raise AuthenticationFailed(f"Failed to exchange authorization code: {str(e)}")

    @staticmethod
    def refresh_access_token(refresh_token: str) -> dict:
        """
        Refresh an expired access token.

        Args:
            refresh_token: The refresh token

        Returns:
            Dict with new access_token and expires_in

        Raises:
            AuthenticationFailed: If refresh fails
        """
        data = {
            'client_id': settings.GOOGLE_CLIENT_ID,
            'client_secret': settings.GOOGLE_CLIENT_SECRET,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
        }

        try:
            response = requests.post(GOOGLE_TOKEN_URL, data=data)
            response.raise_for_status()
            token_data = response.json()

            if 'error' in token_data:
                logger.error(f"Token refresh error: {token_data}")
                raise AuthenticationFailed(
                    f"Failed to refresh token: {token_data.get('error_description', token_data['error'])}"
                )

            return {
                'access_token': token_data['access_token'],
                'expires_in': token_data.get('expires_in', 3600),
            }

        except requests.RequestException as e:
            logger.error(f"Token refresh request failed: {e}")
            raise AuthenticationFailed(f"Failed to refresh access token: {str(e)}")

    @staticmethod
    def store_tokens_for_user(user: User, tokens: dict) -> None:
        """
        Store GCP OAuth tokens for a user.

        Args:
            user: User model instance
            tokens: Dict with access_token, refresh_token, expires_in, scope
        """
        expiry = timezone.now() + timedelta(seconds=tokens.get('expires_in', 3600))

        user.set_gcp_tokens(
            access_token=tokens['access_token'],
            refresh_token=tokens.get('refresh_token') or user.get_gcp_refresh_token() or '',
            expiry=expiry,
            scopes=tokens.get('scope', [])
        )
        user.save()

        logger.info(f"Stored GCP tokens for user {user.email}")

    @staticmethod
    def get_valid_credentials(user: User) -> Optional[Credentials]:
        """
        Get valid GCP credentials for a user, refreshing if necessary.

        Args:
            user: User model instance

        Returns:
            google.oauth2.credentials.Credentials or None if user hasn't authorized
        """
        # Check if user is authenticated (not AnonymousUser)
        if not user or not getattr(user, 'is_authenticated', False):
            return None

        if not user.has_bigquery_access():
            return None

        access_token = user.get_gcp_access_token()
        refresh_token = user.get_gcp_refresh_token()

        if not access_token or not refresh_token:
            return None

        # Check if token is expired or will expire in next 5 minutes
        token_expiry = user.gcp_token_expiry
        if token_expiry and token_expiry <= timezone.now() + timedelta(minutes=5):
            # Refresh the token
            try:
                new_tokens = GCPOAuthService.refresh_access_token(refresh_token)
                GCPOAuthService.store_tokens_for_user(user, new_tokens)
                access_token = new_tokens['access_token']
                token_expiry = timezone.now() + timedelta(seconds=new_tokens.get('expires_in', 3600))
            except AuthenticationFailed:
                # Refresh failed - user needs to re-authorize
                logger.warning(f"Failed to refresh token for user {user.email}")
                user.clear_gcp_tokens()
                user.save()
                return None

        # Create credentials object
        credentials = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=GOOGLE_TOKEN_URL,
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            scopes=user.gcp_scopes,
        )

        return credentials

    @staticmethod
    def get_bigquery_client(user: User, project_id: str = None) -> Optional[bigquery.Client]:
        """
        Get a BigQuery client using the user's credentials.

        Args:
            user: User model instance
            project_id: Optional GCP project ID for billing

        Returns:
            BigQuery client or None if user hasn't authorized

        Raises:
            AuthenticationFailed: If user hasn't granted BigQuery access
        """
        credentials = GCPOAuthService.get_valid_credentials(user)

        if not credentials:
            raise AuthenticationFailed(
                "BigQuery access not authorized. Please authorize access to continue."
            )

        return bigquery.Client(
            project=project_id,
            credentials=credentials
        )

    @staticmethod
    def list_user_projects(user: User) -> list:
        """
        List GCP projects accessible to the user.

        Args:
            user: User model instance

        Returns:
            List of project dicts with id, name, number
        """
        credentials = GCPOAuthService.get_valid_credentials(user)

        if not credentials:
            raise AuthenticationFailed(
                "GCP access not authorized. Please authorize access to list projects."
            )

        try:
            # Use REST API to list projects (more compatible than client library)
            access_token = user.get_gcp_access_token()
            response = requests.get(
                'https://cloudresourcemanager.googleapis.com/v1/projects',
                headers={'Authorization': f'Bearer {access_token}'},
                params={'filter': 'lifecycleState:ACTIVE'}
            )
            response.raise_for_status()
            data = response.json()

            projects = []
            for project in data.get('projects', []):
                projects.append({
                    'project_id': project.get('projectId'),
                    'name': project.get('name'),
                    'project_number': project.get('projectNumber'),
                })

            return projects

        except requests.RequestException as e:
            logger.error(f"Failed to list projects for user {user.email}: {e}")
            raise AuthenticationFailed(f"Failed to list GCP projects: {str(e)}")

    @staticmethod
    def revoke_access(user: User) -> bool:
        """
        Revoke user's GCP OAuth access.

        Args:
            user: User model instance

        Returns:
            True if revocation was successful
        """
        access_token = user.get_gcp_access_token()

        if access_token:
            try:
                # Revoke the token with Google
                response = requests.post(
                    'https://oauth2.googleapis.com/revoke',
                    params={'token': access_token},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                # Don't raise on error - just log it
                if not response.ok:
                    logger.warning(f"Token revocation returned {response.status_code}")
            except requests.RequestException as e:
                logger.warning(f"Token revocation request failed: {e}")

        # Clear tokens from user regardless of revocation result
        user.clear_gcp_tokens()
        user.save()

        logger.info(f"Cleared GCP tokens for user {user.email}")
        return True
