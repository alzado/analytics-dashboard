"""
Serializers for analytics API responses.
"""
from rest_framework import serializers


class PivotRowSerializer(serializers.Serializer):
    """Serializer for pivot table row."""
    dimension_value = serializers.CharField(allow_null=True)
    metrics = serializers.DictField()
    percentage_of_total = serializers.FloatField(default=0.0)
    search_term_count = serializers.IntegerField(default=0)
    has_children = serializers.BooleanField(default=False)


class PivotResponseSerializer(serializers.Serializer):
    """Serializer for pivot table response."""
    rows = PivotRowSerializer(many=True)
    total = PivotRowSerializer(required=False, allow_null=True)
    available_dimensions = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        default=list
    )
    total_count = serializers.IntegerField(required=False, allow_null=True)
    metric_warnings = serializers.DictField(required=False, allow_null=True)
    baseline_totals = serializers.DictField(required=False, allow_null=True)
    error = serializers.CharField(required=False, allow_null=True)
    error_type = serializers.CharField(required=False, allow_null=True)


class PivotChildRowSerializer(serializers.Serializer):
    """Serializer for pivot child row (search term)."""
    search_term = serializers.CharField()
    metrics = serializers.DictField()


class TableInfoSerializer(serializers.Serializer):
    """Serializer for table info response."""
    date_range = serializers.DictField()
    total_rows = serializers.IntegerField()
    schema_fields = serializers.IntegerField(required=False)


class FilterParamsSerializer(serializers.Serializer):
    """Serializer for filter parameters."""
    start_date = serializers.DateField(required=False, allow_null=True)
    end_date = serializers.DateField(required=False, allow_null=True)
    date_range_type = serializers.ChoiceField(
        choices=['absolute', 'relative'],
        default='absolute'
    )
    relative_date_preset = serializers.CharField(required=False, allow_null=True)
    dimension_filters = serializers.DictField(required=False, default=dict)


# =============================================================================
# Significance Testing Serializers
# =============================================================================

class ColumnDefinitionSerializer(serializers.Serializer):
    """Definition of a column for significance testing."""
    column_index = serializers.IntegerField(
        help_text="Index of the column in the pivot table"
    )
    dimension_filters = serializers.DictField(
        child=serializers.ListField(child=serializers.CharField()),
        help_text="Dimension filters that define this column (e.g., {'country': ['USA']})"
    )


class RowDefinitionSerializer(serializers.Serializer):
    """Definition of a row for per-row significance testing."""
    row_id = serializers.CharField(
        help_text="Unique identifier for this row (e.g., dimension value)"
    )
    dimension_filters = serializers.DictField(
        child=serializers.ListField(child=serializers.CharField()),
        required=False,
        default=dict,
        help_text="Dimension filters that define this row"
    )


class SignificanceRequestSerializer(serializers.Serializer):
    """Request for Bayesian significance analysis."""
    control_column = ColumnDefinitionSerializer(
        help_text="The reference/control column to compare against"
    )
    treatment_columns = ColumnDefinitionSerializer(
        many=True,
        help_text="Treatment columns to compare with control"
    )
    metric_ids = serializers.ListField(
        child=serializers.CharField(),
        help_text="Metric IDs to analyze"
    )
    filters = FilterParamsSerializer(
        help_text="Base filters (date range, etc.)"
    )
    rows = RowDefinitionSerializer(
        many=True,
        required=False,
        allow_null=True,
        help_text="Optional list of rows to test. If provided, runs per-row significance tests."
    )


class SignificanceResultItemSerializer(serializers.Serializer):
    """Result for one metric/column/row comparison using proportion-based testing."""
    metric_id = serializers.CharField()
    column_index = serializers.IntegerField()
    row_id = serializers.CharField(allow_null=True, required=False)
    prob_beat_control = serializers.FloatField(
        min_value=0, max_value=1,
        help_text="Probability that treatment beats control (0-1)"
    )
    credible_interval_lower = serializers.FloatField(
        help_text="Lower bound of 95% confidence interval for difference in proportions"
    )
    credible_interval_upper = serializers.FloatField(
        help_text="Upper bound of 95% confidence interval for difference in proportions"
    )
    mean_difference = serializers.FloatField(
        help_text="Difference in proportions (treatment - control)"
    )
    relative_difference = serializers.FloatField(
        help_text="Relative difference as decimal (0.05 = 5% improvement)"
    )
    is_significant = serializers.BooleanField(
        help_text="Whether the difference is statistically significant at 95% threshold"
    )
    direction = serializers.ChoiceField(
        choices=['better', 'worse', 'neutral'],
        help_text="Direction of the effect"
    )
    control_mean = serializers.FloatField(
        help_text="Control proportion (successes/trials)"
    )
    treatment_mean = serializers.FloatField(
        help_text="Treatment proportion (successes/trials)"
    )
    n_control_events = serializers.IntegerField(
        help_text="Number of events (denominator/trials) for control group"
    )
    n_treatment_events = serializers.IntegerField(
        help_text="Number of events (denominator/trials) for treatment group"
    )
    control_successes = serializers.IntegerField(
        help_text="Number of successes (numerator) for control group"
    )
    treatment_successes = serializers.IntegerField(
        help_text="Number of successes (numerator) for treatment group"
    )
    warning = serializers.CharField(
        allow_null=True, required=False,
        help_text="Optional warning message"
    )


class SignificanceResponseSerializer(serializers.Serializer):
    """Response containing all significance results."""
    control_column_index = serializers.IntegerField(
        help_text="Index of the control column"
    )
    results = serializers.DictField(
        child=SignificanceResultItemSerializer(many=True),
        help_text="Results per metric: {metric_id: [results per treatment column]}"
    )
