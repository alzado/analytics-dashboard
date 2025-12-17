"""
Custom User model for the application.
"""
import uuid
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin


class UserManager(BaseUserManager):
    """Custom user manager."""

    def create_user(self, email, name, google_id=None, **extra_fields):
        """Create and save a regular user."""
        if not email:
            raise ValueError('The Email field must be set')
        email = self.normalize_email(email)
        user = self.model(email=email, name=name, google_id=google_id, **extra_fields)
        user.set_unusable_password()  # Users authenticate via OAuth
        user.save(using=self._db)
        return user

    def create_superuser(self, email, name, password=None, **extra_fields):
        """Create and save a superuser."""
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True.')
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')

        user = self.create_user(email, name, **extra_fields)
        if password:
            user.set_password(password)
            user.save(using=self._db)
        return user


class User(AbstractBaseUser, PermissionsMixin):
    """Custom User model with Google OAuth integration."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    google_id = models.CharField(max_length=255, unique=True, null=True, blank=True)
    email = models.EmailField(unique=True)
    name = models.CharField(max_length=255)
    avatar_url = models.URLField(max_length=500, blank=True, null=True)

    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_login_at = models.DateTimeField(null=True, blank=True)

    # GCP OAuth tokens for BigQuery access (encrypted)
    gcp_access_token = models.BinaryField(null=True, blank=True)
    gcp_refresh_token = models.BinaryField(null=True, blank=True)
    gcp_token_expiry = models.DateTimeField(null=True, blank=True)
    gcp_scopes = models.JSONField(default=list, blank=True)  # List of granted scopes

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['name']

    class Meta:
        db_table = 'users'
        indexes = [
            models.Index(fields=['email']),
            models.Index(fields=['google_id']),
        ]

    def __str__(self):
        return self.email

    def get_full_name(self):
        return self.name

    def get_short_name(self):
        return self.name.split()[0] if self.name else self.email

    def has_bigquery_access(self) -> bool:
        """Check if user has granted BigQuery access."""
        if not self.gcp_refresh_token:
            return False
        required_scope = 'https://www.googleapis.com/auth/bigquery'
        return required_scope in (self.gcp_scopes or [])

    def set_gcp_tokens(self, access_token: str, refresh_token: str, expiry, scopes: list):
        """Store encrypted GCP OAuth tokens."""
        from django.conf import settings
        from cryptography.fernet import Fernet

        key = settings.ENCRYPTION_KEY
        if key:
            fernet = Fernet(key.encode() if isinstance(key, str) else key)
            self.gcp_access_token = fernet.encrypt(access_token.encode())
            self.gcp_refresh_token = fernet.encrypt(refresh_token.encode())
        else:
            # Development mode - store unencrypted
            self.gcp_access_token = access_token.encode()
            self.gcp_refresh_token = refresh_token.encode()

        self.gcp_token_expiry = expiry
        self.gcp_scopes = scopes

    def get_gcp_access_token(self) -> str | None:
        """Get decrypted GCP access token."""
        if not self.gcp_access_token:
            return None

        from django.conf import settings
        from cryptography.fernet import Fernet

        key = settings.ENCRYPTION_KEY
        if key:
            fernet = Fernet(key.encode() if isinstance(key, str) else key)
            return fernet.decrypt(bytes(self.gcp_access_token)).decode()
        else:
            return bytes(self.gcp_access_token).decode()

    def get_gcp_refresh_token(self) -> str | None:
        """Get decrypted GCP refresh token."""
        if not self.gcp_refresh_token:
            return None

        from django.conf import settings
        from cryptography.fernet import Fernet

        key = settings.ENCRYPTION_KEY
        if key:
            fernet = Fernet(key.encode() if isinstance(key, str) else key)
            return fernet.decrypt(bytes(self.gcp_refresh_token)).decode()
        else:
            return bytes(self.gcp_refresh_token).decode()

    def clear_gcp_tokens(self):
        """Remove GCP OAuth tokens."""
        self.gcp_access_token = None
        self.gcp_refresh_token = None
        self.gcp_token_expiry = None
        self.gcp_scopes = []
