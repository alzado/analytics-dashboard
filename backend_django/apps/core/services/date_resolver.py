"""
Date resolver service for converting relative date presets to absolute dates.
Django port of the FastAPI date_resolver.py.
"""
from datetime import date, timedelta
from enum import Enum
from typing import Tuple, Optional
import calendar


class DatePreset(str, Enum):
    """Supported relative date presets."""
    TODAY = 'today'
    YESTERDAY = 'yesterday'
    LAST_7_DAYS = 'last_7_days'
    LAST_14_DAYS = 'last_14_days'
    LAST_30_DAYS = 'last_30_days'
    LAST_90_DAYS = 'last_90_days'
    THIS_WEEK = 'this_week'
    LAST_WEEK = 'last_week'
    THIS_MONTH = 'this_month'
    LAST_MONTH = 'last_month'
    THIS_QUARTER = 'this_quarter'
    LAST_QUARTER = 'last_quarter'
    THIS_YEAR = 'this_year'
    LAST_YEAR = 'last_year'
    YEAR_TO_DATE = 'year_to_date'
    MONTH_TO_DATE = 'month_to_date'
    QUARTER_TO_DATE = 'quarter_to_date'
    WEEK_TO_DATE = 'week_to_date'


class DateResolver:
    """Service for resolving relative date presets to absolute date ranges."""

    @staticmethod
    def get_quarter(d: date) -> int:
        """Get the quarter (1-4) for a date."""
        return (d.month - 1) // 3 + 1

    @staticmethod
    def get_quarter_start(year: int, quarter: int) -> date:
        """Get the first day of a quarter."""
        month = (quarter - 1) * 3 + 1
        return date(year, month, 1)

    @staticmethod
    def get_quarter_end(year: int, quarter: int) -> date:
        """Get the last day of a quarter."""
        month = quarter * 3
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)

    @staticmethod
    def resolve(preset: str, reference_date: Optional[date] = None) -> Tuple[str, str]:
        """
        Resolve a relative date preset to absolute start and end dates.

        Args:
            preset: The date preset string (e.g., 'last_7_days', 'this_month')
            reference_date: Reference date for calculations (defaults to today)

        Returns:
            Tuple of (start_date, end_date) as 'YYYY-MM-DD' strings

        Raises:
            ValueError: If preset is not recognized
        """
        today = reference_date or date.today()

        if preset == DatePreset.TODAY:
            return today.isoformat(), today.isoformat()

        elif preset == DatePreset.YESTERDAY:
            yesterday = today - timedelta(days=1)
            return yesterday.isoformat(), yesterday.isoformat()

        elif preset == DatePreset.LAST_7_DAYS:
            start = today - timedelta(days=6)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_14_DAYS:
            start = today - timedelta(days=13)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_30_DAYS:
            start = today - timedelta(days=29)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_90_DAYS:
            start = today - timedelta(days=89)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.THIS_WEEK:
            # Week starts on Monday (weekday() returns 0 for Monday)
            start = today - timedelta(days=today.weekday())
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_WEEK:
            # Last week: Monday to Sunday
            days_since_monday = today.weekday()
            last_monday = today - timedelta(days=days_since_monday + 7)
            last_sunday = last_monday + timedelta(days=6)
            return last_monday.isoformat(), last_sunday.isoformat()

        elif preset == DatePreset.THIS_MONTH:
            start = date(today.year, today.month, 1)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_MONTH:
            # First day of last month
            if today.month == 1:
                start = date(today.year - 1, 12, 1)
                last_day = 31
            else:
                start = date(today.year, today.month - 1, 1)
                last_day = calendar.monthrange(today.year, today.month - 1)[1]
            end = date(start.year, start.month, last_day)
            return start.isoformat(), end.isoformat()

        elif preset == DatePreset.THIS_QUARTER:
            quarter = DateResolver.get_quarter(today)
            start = DateResolver.get_quarter_start(today.year, quarter)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_QUARTER:
            current_quarter = DateResolver.get_quarter(today)
            if current_quarter == 1:
                last_quarter = 4
                year = today.year - 1
            else:
                last_quarter = current_quarter - 1
                year = today.year
            start = DateResolver.get_quarter_start(year, last_quarter)
            end = DateResolver.get_quarter_end(year, last_quarter)
            return start.isoformat(), end.isoformat()

        elif preset == DatePreset.THIS_YEAR:
            start = date(today.year, 1, 1)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.LAST_YEAR:
            start = date(today.year - 1, 1, 1)
            end = date(today.year - 1, 12, 31)
            return start.isoformat(), end.isoformat()

        elif preset == DatePreset.YEAR_TO_DATE:
            start = date(today.year, 1, 1)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.MONTH_TO_DATE:
            start = date(today.year, today.month, 1)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.QUARTER_TO_DATE:
            quarter = DateResolver.get_quarter(today)
            start = DateResolver.get_quarter_start(today.year, quarter)
            return start.isoformat(), today.isoformat()

        elif preset == DatePreset.WEEK_TO_DATE:
            start = today - timedelta(days=today.weekday())
            return start.isoformat(), today.isoformat()

        else:
            raise ValueError(f"Unknown date preset: {preset}")

    @staticmethod
    def get_available_presets() -> list:
        """Get list of available date presets with descriptions."""
        return [
            {'id': DatePreset.TODAY, 'name': 'Today', 'description': "Today's date only"},
            {'id': DatePreset.YESTERDAY, 'name': 'Yesterday', 'description': "Yesterday's date only"},
            {'id': DatePreset.LAST_7_DAYS, 'name': 'Last 7 Days', 'description': 'Past 7 days including today'},
            {'id': DatePreset.LAST_14_DAYS, 'name': 'Last 14 Days', 'description': 'Past 14 days including today'},
            {'id': DatePreset.LAST_30_DAYS, 'name': 'Last 30 Days', 'description': 'Past 30 days including today'},
            {'id': DatePreset.LAST_90_DAYS, 'name': 'Last 90 Days', 'description': 'Past 90 days including today'},
            {'id': DatePreset.THIS_WEEK, 'name': 'This Week', 'description': 'Monday to today'},
            {'id': DatePreset.LAST_WEEK, 'name': 'Last Week', 'description': 'Last Monday to Sunday'},
            {'id': DatePreset.THIS_MONTH, 'name': 'This Month', 'description': '1st of month to today'},
            {'id': DatePreset.LAST_MONTH, 'name': 'Last Month', 'description': 'Complete previous month'},
            {'id': DatePreset.THIS_QUARTER, 'name': 'This Quarter', 'description': 'Start of quarter to today'},
            {'id': DatePreset.LAST_QUARTER, 'name': 'Last Quarter', 'description': 'Complete previous quarter'},
            {'id': DatePreset.THIS_YEAR, 'name': 'This Year', 'description': 'January 1st to today'},
            {'id': DatePreset.LAST_YEAR, 'name': 'Last Year', 'description': 'Complete previous year'},
            {'id': DatePreset.YEAR_TO_DATE, 'name': 'Year to Date', 'description': 'January 1st to today'},
            {'id': DatePreset.MONTH_TO_DATE, 'name': 'Month to Date', 'description': '1st of month to today'},
            {'id': DatePreset.QUARTER_TO_DATE, 'name': 'Quarter to Date', 'description': 'Start of quarter to today'},
            {'id': DatePreset.WEEK_TO_DATE, 'name': 'Week to Date', 'description': 'Monday to today'},
        ]


def resolve_relative_date(preset: str, reference_date: Optional[date] = None) -> Tuple[str, str]:
    """
    Convenience function to resolve a relative date preset.

    Args:
        preset: The date preset string
        reference_date: Reference date for calculations (defaults to today)

    Returns:
        Tuple of (start_date, end_date) as 'YYYY-MM-DD' strings
    """
    return DateResolver.resolve(preset, reference_date)
