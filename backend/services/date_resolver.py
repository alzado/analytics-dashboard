"""
Date resolution service for converting relative date presets to absolute dates.

This service handles the conversion of relative date ranges (e.g., "last_7_days", "this_month")
into absolute start_date and end_date values in YYYY-MM-DD format.
"""

from datetime import datetime, timedelta
from typing import Tuple, Optional
from enum import Enum


class RelativeDatePreset(str, Enum):
    """Supported relative date presets."""
    TODAY = "today"
    YESTERDAY = "yesterday"
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"
    LAST_90_DAYS = "last_90_days"
    THIS_WEEK = "this_week"
    LAST_WEEK = "last_week"
    THIS_MONTH = "this_month"
    LAST_MONTH = "last_month"
    THIS_QUARTER = "this_quarter"
    LAST_QUARTER = "last_quarter"
    THIS_YEAR = "this_year"
    LAST_YEAR = "last_year"


class DateResolver:
    """Service for resolving relative date presets to absolute date ranges."""

    @staticmethod
    def resolve_relative_date(preset: str, reference_date: Optional[datetime] = None) -> Tuple[str, str]:
        """
        Resolve a relative date preset to absolute start_date and end_date.

        Args:
            preset: The relative date preset (e.g., "last_7_days")
            reference_date: Optional reference date (defaults to today)

        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format

        Raises:
            ValueError: If preset is not recognized
        """
        if reference_date is None:
            reference_date = datetime.now()

        # Normalize to start of day
        today = reference_date.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            preset_enum = RelativeDatePreset(preset)
        except ValueError:
            raise ValueError(f"Unknown relative date preset: {preset}")

        # Simple date calculations
        if preset_enum == RelativeDatePreset.TODAY:
            return (
                today.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.YESTERDAY:
            yesterday = today - timedelta(days=1)
            return (
                yesterday.strftime("%Y-%m-%d"),
                yesterday.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_7_DAYS:
            start = today - timedelta(days=7)
            return (
                start.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_30_DAYS:
            start = today - timedelta(days=30)
            return (
                start.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_90_DAYS:
            start = today - timedelta(days=90)
            return (
                start.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d")
            )

        # Week calculations (Monday = 0, Sunday = 6)
        elif preset_enum == RelativeDatePreset.THIS_WEEK:
            # Find Monday of current week
            days_since_monday = today.weekday()  # 0=Monday, 6=Sunday
            week_start = today - timedelta(days=days_since_monday)
            week_end = week_start + timedelta(days=6)
            return (
                week_start.strftime("%Y-%m-%d"),
                week_end.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_WEEK:
            # Find Monday of last week
            days_since_monday = today.weekday()
            this_week_monday = today - timedelta(days=days_since_monday)
            last_week_monday = this_week_monday - timedelta(days=7)
            last_week_sunday = last_week_monday + timedelta(days=6)
            return (
                last_week_monday.strftime("%Y-%m-%d"),
                last_week_sunday.strftime("%Y-%m-%d")
            )

        # Month calculations
        elif preset_enum == RelativeDatePreset.THIS_MONTH:
            month_start = today.replace(day=1)
            # Get last day of month
            if today.month == 12:
                next_month = today.replace(year=today.year + 1, month=1, day=1)
            else:
                next_month = today.replace(month=today.month + 1, day=1)
            month_end = next_month - timedelta(days=1)
            return (
                month_start.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_MONTH:
            # Get first day of last month
            if today.month == 1:
                last_month_start = today.replace(year=today.year - 1, month=12, day=1)
            else:
                last_month_start = today.replace(month=today.month - 1, day=1)

            # Get last day of last month (= day before first day of this month)
            this_month_start = today.replace(day=1)
            last_month_end = this_month_start - timedelta(days=1)

            return (
                last_month_start.strftime("%Y-%m-%d"),
                last_month_end.strftime("%Y-%m-%d")
            )

        # Quarter calculations (Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec)
        elif preset_enum == RelativeDatePreset.THIS_QUARTER:
            quarter_month = ((today.month - 1) // 3) * 3 + 1  # First month of quarter
            quarter_start = today.replace(month=quarter_month, day=1)

            # Last day of quarter
            if quarter_month == 10:  # Q4
                quarter_end = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                next_quarter_month = quarter_month + 3
                quarter_end = today.replace(month=next_quarter_month, day=1) - timedelta(days=1)

            return (
                quarter_start.strftime("%Y-%m-%d"),
                quarter_end.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_QUARTER:
            current_quarter_month = ((today.month - 1) // 3) * 3 + 1

            # Get first month of last quarter
            if current_quarter_month == 1:  # Currently Q1, last was Q4
                last_quarter_month = 10
                last_quarter_year = today.year - 1
            else:
                last_quarter_month = current_quarter_month - 3
                last_quarter_year = today.year

            last_quarter_start = today.replace(year=last_quarter_year, month=last_quarter_month, day=1)

            # End is day before current quarter starts
            current_quarter_start = today.replace(month=current_quarter_month, day=1)
            last_quarter_end = current_quarter_start - timedelta(days=1)

            return (
                last_quarter_start.strftime("%Y-%m-%d"),
                last_quarter_end.strftime("%Y-%m-%d")
            )

        # Year calculations
        elif preset_enum == RelativeDatePreset.THIS_YEAR:
            year_start = today.replace(month=1, day=1)
            year_end = today.replace(month=12, day=31)
            return (
                year_start.strftime("%Y-%m-%d"),
                year_end.strftime("%Y-%m-%d")
            )

        elif preset_enum == RelativeDatePreset.LAST_YEAR:
            last_year = today.year - 1
            year_start = today.replace(year=last_year, month=1, day=1)
            year_end = today.replace(year=last_year, month=12, day=31)
            return (
                year_start.strftime("%Y-%m-%d"),
                year_end.strftime("%Y-%m-%d")
            )

        else:
            raise ValueError(f"Unhandled relative date preset: {preset}")


# Convenience function for easy imports
def resolve_relative_date(preset: str, reference_date: Optional[datetime] = None) -> Tuple[str, str]:
    """
    Convenience function to resolve relative dates.

    Args:
        preset: The relative date preset (e.g., "last_7_days")
        reference_date: Optional reference date (defaults to today)

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    return DateResolver.resolve_relative_date(preset, reference_date)
