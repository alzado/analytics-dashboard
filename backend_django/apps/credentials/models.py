"""
GCP Credentials models.
"""
import uuid
import json
from django.db import models
from django.conf import settings
from cryptography.fernet import Fernet
import base64
import os


class CredentialType(models.TextChoices):
    SERVICE_ACCOUNT = 'service_account', 'Service Account'
    OAUTH = 'oauth', 'OAuth'


def get_encryption_key():
    """Get or generate encryption key."""
    key = settings.ENCRYPTION_KEY
    if key:
        return key.encode() if isinstance(key, str) else key
    # Generate a key if not set (for development)
    return Fernet.generate_key()


class GCPCredential(models.Model):
    """Encrypted GCP credentials storage."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='gcp_credentials',
        null=True, blank=True
    )
    organization = models.ForeignKey(
        'organizations.Organization',
        on_delete=models.CASCADE,
        related_name='gcp_credentials',
        null=True, blank=True
    )

    name = models.CharField(max_length=255)
    credential_type = models.CharField(
        max_length=20,
        choices=CredentialType.choices,
        default=CredentialType.SERVICE_ACCOUNT
    )
    encrypted_credentials = models.BinaryField()
    project_id = models.CharField(max_length=255)
    is_default = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'gcp_credentials'
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['organization']),
        ]

    def __str__(self):
        return f"{self.name} ({self.project_id})"

    def set_credentials(self, credentials_json: str):
        """Encrypt and store credentials."""
        fernet = Fernet(get_encryption_key())
        encrypted = fernet.encrypt(credentials_json.encode())
        self.encrypted_credentials = encrypted

    def get_credentials(self) -> dict:
        """Decrypt and return credentials."""
        fernet = Fernet(get_encryption_key())
        decrypted = fernet.decrypt(bytes(self.encrypted_credentials))
        return json.loads(decrypted.decode())

    def save(self, *args, **kwargs):
        # If setting as default, unset other defaults for same user/org
        if self.is_default:
            if self.user:
                GCPCredential.objects.filter(
                    user=self.user, is_default=True
                ).exclude(id=self.id).update(is_default=False)
            if self.organization:
                GCPCredential.objects.filter(
                    organization=self.organization, is_default=True
                ).exclude(id=self.id).update(is_default=False)
        super().save(*args, **kwargs)
