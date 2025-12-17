"""
User authentication views.
"""
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from apps.core.permissions import IsAuthenticatedOrAuthDisabled

from .serializers import (
    GoogleAuthSerializer,
    RefreshTokenSerializer,
    UserSerializer,
    UserProfileUpdateSerializer,
    AuthTokenResponseSerializer,
    GCPOAuthCallbackSerializer,
    GCPOAuthStatusSerializer,
    GCPProjectSerializer,
)
from .services import AuthService
from .gcp_oauth_service import GCPOAuthService


class GoogleAuthView(APIView):
    """Google OAuth authentication endpoint."""
    permission_classes = [AllowAny]

    def post(self, request):
        """
        Authenticate with Google OAuth.

        POST /api/auth/google/
        Body: {"token": "google_id_token"}

        Returns: {access_token, refresh_token, token_type, user}
        """
        serializer = GoogleAuthSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        result = AuthService.authenticate_google(serializer.validated_data['id_token'])

        return Response({
            'access_token': result['access_token'],
            'refresh_token': result['refresh_token'],
            'token_type': result['token_type'],
            'user': UserSerializer(result['user']).data
        })


class RefreshTokenView(APIView):
    """Token refresh endpoint."""
    permission_classes = [AllowAny]

    def post(self, request):
        """
        Refresh access token.

        POST /api/auth/refresh/
        Body: {"refresh_token": "jwt_refresh_token"}

        Returns: {access_token, refresh_token, token_type, user}
        """
        serializer = RefreshTokenSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        result = AuthService.refresh_access_token(serializer.validated_data['refresh_token'])

        return Response({
            'access_token': result['access_token'],
            'refresh_token': result['refresh_token'],
            'token_type': result['token_type'],
            'user': UserSerializer(result['user']).data
        })


class CurrentUserView(APIView):
    """Current user profile endpoint."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def get(self, request):
        """
        Get current user profile.

        GET /api/auth/me/

        Returns: User object
        """
        serializer = UserSerializer(request.user)
        return Response(serializer.data)

    def put(self, request):
        """
        Update current user profile.

        PUT /api/auth/me/
        Body: {"name": "New Name", "avatar_url": "https://..."}

        Returns: Updated user object
        """
        serializer = UserProfileUpdateSerializer(
            request.user,
            data=request.data,
            partial=True
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return Response(UserSerializer(request.user).data)


class GCPAuthUrlView(APIView):
    """Get GCP OAuth authorization URL."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def get(self, request):
        """
        Get URL to authorize BigQuery access.

        GET /api/auth/gcp/authorize/

        Returns: {authorization_url: "https://accounts.google.com/..."}
        """
        state = str(request.user.id)  # Use user ID as state for verification
        redirect_uri = request.query_params.get('redirect_uri')

        auth_url = GCPOAuthService.get_authorization_url(
            state=state,
            redirect_uri=redirect_uri
        )

        return Response({'authorization_url': auth_url})


class GCPAuthCallbackView(APIView):
    """Handle GCP OAuth callback."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def post(self, request):
        """
        Exchange authorization code for tokens.

        POST /api/auth/gcp/callback/
        Body: {"code": "auth_code", "redirect_uri": "..."}

        Returns: {success: true, has_bigquery_access: true}
        """
        serializer = GCPOAuthCallbackSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        code = serializer.validated_data['code']
        redirect_uri = serializer.validated_data.get('redirect_uri')

        # Exchange code for tokens
        tokens = GCPOAuthService.exchange_code_for_tokens(
            code=code,
            redirect_uri=redirect_uri
        )

        # Store tokens for user
        GCPOAuthService.store_tokens_for_user(request.user, tokens)

        return Response({
            'success': True,
            'has_bigquery_access': request.user.has_bigquery_access(),
            'scopes': tokens.get('scope', [])
        })


class GCPAuthStatusView(APIView):
    """Get GCP OAuth status for current user."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def get(self, request):
        """
        Get BigQuery authorization status.

        GET /api/auth/gcp/status/

        Returns: {has_bigquery_access: bool, scopes: [...], token_expiry: ...}
        """
        user = request.user

        return Response({
            'has_bigquery_access': user.has_bigquery_access(),
            'scopes': user.gcp_scopes or [],
            'token_expiry': user.gcp_token_expiry,
        })


class GCPAuthRevokeView(APIView):
    """Revoke GCP OAuth access."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def post(self, request):
        """
        Revoke BigQuery access.

        POST /api/auth/gcp/revoke/

        Returns: {success: true}
        """
        GCPOAuthService.revoke_access(request.user)

        return Response({
            'success': True,
            'has_bigquery_access': False
        })


class GCPProjectsView(APIView):
    """List GCP projects accessible to user."""
    permission_classes = [IsAuthenticatedOrAuthDisabled]

    def get(self, request):
        """
        List GCP projects user has access to.

        GET /api/auth/gcp/projects/

        Returns: [{project_id, name, project_number}, ...]
        """
        projects = GCPOAuthService.list_user_projects(request.user)
        serializer = GCPProjectSerializer(projects, many=True)

        return Response(serializer.data)
