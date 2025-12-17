"""
Statistical significance testing service using Bayesian methods.
Compares daily metric distributions between control and treatment groups.

Ported from FastAPI backend/services/statistical_service.py.
"""
import numpy as np
from scipy import stats
from typing import Dict, List, Optional, Literal
from dataclasses import dataclass, asdict


@dataclass
class SignificanceResult:
    """Result of a significance test between two groups."""
    metric_id: str
    column_index: int
    prob_beat_control: float
    credible_interval_lower: float
    credible_interval_upper: float
    mean_difference: float
    relative_difference: float
    is_significant: bool
    direction: Literal["better", "worse", "neutral"]
    control_mean: float
    treatment_mean: float
    n_days: int
    warning: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class ProportionSignificanceResult:
    """Result of a proportion-based significance test (two-proportion z-test)."""
    metric_id: str
    column_index: int
    prob_beat_control: float
    credible_interval_lower: float  # CI for difference in proportions
    credible_interval_upper: float
    mean_difference: float  # Difference in proportions (treatment - control)
    relative_difference: float  # Relative change as decimal
    is_significant: bool
    direction: Literal["better", "worse", "neutral"]
    control_mean: float  # Control proportion (successes/trials)
    treatment_mean: float  # Treatment proportion
    n_control_events: int  # Denominator count for control (trials)
    n_treatment_events: int  # Denominator count for treatment
    control_successes: int  # Numerator count for control
    treatment_successes: int  # Numerator count for treatment
    warning: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class StatisticalService:
    """
    Bayesian significance testing using daily observations.

    Compares distributions of daily metric values between control and treatment.
    Each day is treated as an independent observation, allowing us to estimate
    variance from actual day-to-day fluctuations.

    Statistical Model:
    - Normal-Normal model for comparing means
    - Uses t-distribution for small samples (< 30 days)
    - Monte Carlo sampling for computing P(treatment > control)
    """

    DEFAULT_N_SAMPLES = 10000
    SIGNIFICANCE_THRESHOLD = 0.95
    MIN_DAYS_FOR_NORMAL = 30
    MIN_DAYS_WARNING = 7

    def __init__(self, n_samples: int = DEFAULT_N_SAMPLES):
        self.n_samples = n_samples

    def bayesian_daily_comparison(
        self,
        control_daily_values: List[float],
        treatment_daily_values: List[float],
        higher_is_better: bool = True
    ) -> dict:
        """
        Compare two groups using their daily observations.

        Uses Normal-Normal model:
        - Estimate mean and std from daily values
        - For small samples (< 30), use t-distribution
        - Sample from posterior distributions
        - Compute P(treatment > control)

        Args:
            control_daily_values: List of daily metric values for control
            treatment_daily_values: List of daily metric values for treatment
            higher_is_better: If True, higher values are better

        Returns:
            Dict with prob_beat_control, credible_interval, mean_difference, etc.
        """
        # Convert to numpy arrays and filter out NaN/None
        control = np.array([v for v in control_daily_values if v is not None and not np.isnan(v)])
        treatment = np.array([v for v in treatment_daily_values if v is not None and not np.isnan(v)])

        n_control = len(control)
        n_treatment = len(treatment)

        # Handle edge cases
        if n_control < 2 or n_treatment < 2:
            return {
                'prob_beat_control': 0.5,
                'credible_interval_lower': 0.0,
                'credible_interval_upper': 0.0,
                'mean_difference': 0.0,
                'relative_difference': 0.0,
                'is_significant': False,
                'direction': 'neutral',
                'control_mean': float(np.mean(control)) if n_control > 0 else 0.0,
                'treatment_mean': float(np.mean(treatment)) if n_treatment > 0 else 0.0,
                'n_days': min(n_control, n_treatment),
                'warning': 'Insufficient data (need at least 2 days per group)'
            }

        # Calculate sample statistics
        control_mean = np.mean(control)
        treatment_mean = np.mean(treatment)
        control_std = np.std(control, ddof=1)  # Sample std
        treatment_std = np.std(treatment, ddof=1)

        # Prevent zero standard deviation
        min_std = 1e-10
        control_std = max(control_std, min_std)
        treatment_std = max(treatment_std, min_std)

        # Standard error of the mean
        control_sem = control_std / np.sqrt(n_control)
        treatment_sem = treatment_std / np.sqrt(n_treatment)

        # Set random seed for reproducibility
        np.random.seed(42)

        # Sample from posterior distributions
        # For small samples, use t-distribution; for large samples, use normal
        if n_control < self.MIN_DAYS_FOR_NORMAL:
            # Use scaled t-distribution
            control_samples = stats.t.rvs(
                df=n_control - 1,
                loc=control_mean,
                scale=control_sem,
                size=self.n_samples
            )
        else:
            control_samples = np.random.normal(control_mean, control_sem, self.n_samples)

        if n_treatment < self.MIN_DAYS_FOR_NORMAL:
            treatment_samples = stats.t.rvs(
                df=n_treatment - 1,
                loc=treatment_mean,
                scale=treatment_sem,
                size=self.n_samples
            )
        else:
            treatment_samples = np.random.normal(treatment_mean, treatment_sem, self.n_samples)

        # Probability treatment beats control
        if higher_is_better:
            prob_beat_control = np.mean(treatment_samples > control_samples)
        else:
            prob_beat_control = np.mean(treatment_samples < control_samples)

        # Difference distribution (treatment - control)
        diff_samples = treatment_samples - control_samples

        # 95% credible interval for the difference
        ci_lower = float(np.percentile(diff_samples, 2.5))
        ci_upper = float(np.percentile(diff_samples, 97.5))

        # Mean difference
        mean_diff = float(treatment_mean - control_mean)

        # Relative difference (as decimal, e.g., 0.05 = 5% improvement)
        if abs(control_mean) > min_std:
            relative_diff = mean_diff / abs(control_mean)
        else:
            relative_diff = 0.0

        # Determine significance and direction
        is_significant = (
            prob_beat_control >= self.SIGNIFICANCE_THRESHOLD or
            prob_beat_control <= (1 - self.SIGNIFICANCE_THRESHOLD)
        )

        if prob_beat_control >= self.SIGNIFICANCE_THRESHOLD:
            direction = "better" if higher_is_better else "worse"
        elif prob_beat_control <= (1 - self.SIGNIFICANCE_THRESHOLD):
            direction = "worse" if higher_is_better else "better"
        else:
            direction = "neutral"

        # Generate warning for small samples
        warning = None
        min_days = min(n_control, n_treatment)
        if min_days < self.MIN_DAYS_WARNING:
            warning = f"Very small sample size ({min_days} days). Results may be unreliable."
        elif min_days < self.MIN_DAYS_FOR_NORMAL:
            warning = f"Small sample size ({min_days} days). Using t-distribution for more conservative estimates."

        return {
            'prob_beat_control': float(prob_beat_control),
            'credible_interval_lower': ci_lower,
            'credible_interval_upper': ci_upper,
            'mean_difference': mean_diff,
            'relative_difference': float(relative_diff),
            'is_significant': is_significant,
            'direction': direction,
            'control_mean': float(control_mean),
            'treatment_mean': float(treatment_mean),
            'n_days': min_days,
            'warning': warning
        }

    def analyze_metric(
        self,
        metric_id: str,
        control_daily_values: List[float],
        treatment_daily_values: List[float],
        column_index: int,
        higher_is_better: bool = True
    ) -> SignificanceResult:
        """
        Run significance test for a single metric.

        Args:
            metric_id: Identifier for the metric
            control_daily_values: Daily values for control group
            treatment_daily_values: Daily values for treatment group
            column_index: Index of the treatment column
            higher_is_better: Whether higher values are better

        Returns:
            SignificanceResult with all test results
        """
        try:
            result = self.bayesian_daily_comparison(
                control_daily_values,
                treatment_daily_values,
                higher_is_better
            )
        except Exception as e:
            # Return neutral result on error
            return SignificanceResult(
                metric_id=metric_id,
                column_index=column_index,
                prob_beat_control=0.5,
                credible_interval_lower=0.0,
                credible_interval_upper=0.0,
                mean_difference=0.0,
                relative_difference=0.0,
                is_significant=False,
                direction="neutral",
                control_mean=0.0,
                treatment_mean=0.0,
                n_days=0,
                warning=f"Error computing significance: {str(e)}"
            )

        return SignificanceResult(
            metric_id=metric_id,
            column_index=column_index,
            prob_beat_control=result['prob_beat_control'],
            credible_interval_lower=result['credible_interval_lower'],
            credible_interval_upper=result['credible_interval_upper'],
            mean_difference=result['mean_difference'],
            relative_difference=result['relative_difference'],
            is_significant=result['is_significant'],
            direction=result['direction'],
            control_mean=result['control_mean'],
            treatment_mean=result['treatment_mean'],
            n_days=result['n_days'],
            warning=result.get('warning')
        )

    def analyze_all_metrics(
        self,
        metric_ids: List[str],
        control_daily_data: Dict[str, List[float]],
        treatment_columns_daily_data: List[Dict[str, List[float]]],
        column_indices: List[int],
        metric_directions: Optional[Dict[str, bool]] = None
    ) -> Dict[str, List[SignificanceResult]]:
        """
        Analyze all metrics across all treatment columns.

        Args:
            metric_ids: List of metric IDs to analyze
            control_daily_data: Dict mapping metric_id to daily values for control
            treatment_columns_daily_data: List of dicts, each mapping metric_id to daily values
            column_indices: List of column indices corresponding to treatment columns
            metric_directions: Optional dict mapping metric_id to higher_is_better boolean

        Returns:
            Dict mapping metric_id to list of SignificanceResult (one per treatment column)
        """
        if metric_directions is None:
            metric_directions = {}

        results: Dict[str, List[SignificanceResult]] = {}

        for metric_id in metric_ids:
            metric_results = []
            higher_is_better = metric_directions.get(metric_id, self.get_higher_is_better(metric_id))

            control_values = control_daily_data.get(metric_id, [])

            for treatment_data, col_index in zip(treatment_columns_daily_data, column_indices):
                treatment_values = treatment_data.get(metric_id, [])

                result = self.analyze_metric(
                    metric_id=metric_id,
                    control_daily_values=control_values,
                    treatment_daily_values=treatment_values,
                    column_index=col_index,
                    higher_is_better=higher_is_better
                )
                metric_results.append(result)

            results[metric_id] = metric_results

        return results

    def get_higher_is_better(self, metric_id: str) -> bool:
        """
        Determine if higher values are better for this metric.

        Most metrics: higher is better (revenue, conversion, CTR, etc.)
        Some metrics: lower is better (bounce rate, cart abandonment, etc.)

        Args:
            metric_id: Metric identifier

        Returns:
            True if higher values are better
        """
        # Metrics where lower is better
        lower_is_better_keywords = [
            'bounce', 'abandon', 'churn', 'error', 'fail', 'cancel',
            'refund', 'return', 'complaint', 'wait', 'latency', 'cost',
            'exit', 'drop', 'loss'
        ]

        metric_lower = metric_id.lower()
        for keyword in lower_is_better_keywords:
            if keyword in metric_lower:
                return False

        return True

    def proportion_comparison(
        self,
        control_successes: int,
        control_trials: int,
        treatment_successes: int,
        treatment_trials: int,
        higher_is_better: bool = True
    ) -> dict:
        """
        Two-proportion z-test for comparing rates/proportions.

        Uses the standard two-proportion z-test:
        - p1 = x1/n1 (control proportion)
        - p2 = x2/n2 (treatment proportion)
        - p_pooled = (x1 + x2) / (n1 + n2)
        - SE = sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
        - z = (p2 - p1) / SE

        Args:
            control_successes: Numerator count for control (e.g., queries_pdp)
            control_trials: Denominator count for control (e.g., queries)
            treatment_successes: Numerator count for treatment
            treatment_trials: Denominator count for treatment
            higher_is_better: Whether higher proportion is better

        Returns:
            Dict with prob_beat_control, credible_interval, is_significant, etc.
        """
        # Handle edge cases
        if control_trials <= 0 or treatment_trials <= 0:
            return {
                'prob_beat_control': 0.5,
                'credible_interval_lower': 0.0,
                'credible_interval_upper': 0.0,
                'mean_difference': 0.0,
                'relative_difference': 0.0,
                'is_significant': False,
                'direction': 'neutral',
                'control_mean': 0.0,
                'treatment_mean': 0.0,
                'n_control_events': control_trials,
                'n_treatment_events': treatment_trials,
                'control_successes': control_successes,
                'treatment_successes': treatment_successes,
                'warning': 'Insufficient data (zero trials in one or both groups)'
            }

        # Ensure successes don't exceed trials
        control_successes = min(control_successes, control_trials)
        treatment_successes = min(treatment_successes, treatment_trials)

        # Calculate proportions
        p_control = control_successes / control_trials
        p_treatment = treatment_successes / treatment_trials

        # Pooled proportion for standard error calculation
        total_successes = control_successes + treatment_successes
        total_trials = control_trials + treatment_trials
        p_pooled = total_successes / total_trials

        # Standard error of difference in proportions
        # SE = sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
        if p_pooled == 0 or p_pooled == 1:
            # Edge case: all successes or all failures
            se = 0.0
        else:
            se = np.sqrt(p_pooled * (1 - p_pooled) * (1/control_trials + 1/treatment_trials))

        # Difference in proportions
        diff = p_treatment - p_control

        # Calculate z-statistic
        if se > 0:
            z_stat = diff / se
        else:
            z_stat = 0.0

        # Two-tailed p-value
        p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))

        # One-tailed probability (P(treatment > control))
        if higher_is_better:
            prob_beat_control = 1 - stats.norm.cdf(-z_stat)  # P(Z > -z) = P(diff > 0)
        else:
            prob_beat_control = stats.norm.cdf(-z_stat)  # P(Z < -z) = P(diff < 0)

        # 95% confidence interval for difference in proportions
        z_critical = stats.norm.ppf(0.975)  # 1.96 for 95% CI
        ci_lower = diff - z_critical * se
        ci_upper = diff + z_critical * se

        # Relative difference
        if p_control > 0:
            relative_diff = diff / p_control
        else:
            relative_diff = 0.0 if diff == 0 else float('inf')

        # Determine significance (using p < 0.05)
        is_significant = p_value < 0.05

        # Determine direction
        if is_significant:
            if diff > 0:
                direction = "better" if higher_is_better else "worse"
            else:
                direction = "worse" if higher_is_better else "better"
        else:
            direction = "neutral"

        # Generate warnings for small samples
        warning = None
        min_events = min(control_trials, treatment_trials)
        if min_events < 30:
            warning = f"Small sample size ({min_events} events). Normal approximation may be unreliable."
        elif min(control_successes, treatment_successes) < 5:
            warning = "Very few successes in one group. Consider using exact test."

        return {
            'prob_beat_control': float(prob_beat_control),
            'credible_interval_lower': float(ci_lower),
            'credible_interval_upper': float(ci_upper),
            'mean_difference': float(diff),
            'relative_difference': float(relative_diff),
            'is_significant': is_significant,
            'direction': direction,
            'control_mean': float(p_control),
            'treatment_mean': float(p_treatment),
            'n_control_events': control_trials,
            'n_treatment_events': treatment_trials,
            'control_successes': control_successes,
            'treatment_successes': treatment_successes,
            'warning': warning
        }

    def analyze_proportion_metric(
        self,
        metric_id: str,
        control_successes: int,
        control_trials: int,
        treatment_successes: int,
        treatment_trials: int,
        column_index: int,
        higher_is_better: bool = True
    ) -> ProportionSignificanceResult:
        """
        Run proportion-based significance test for a single metric.

        Args:
            metric_id: Identifier for the metric
            control_successes: Numerator count for control
            control_trials: Denominator count for control
            treatment_successes: Numerator count for treatment
            treatment_trials: Denominator count for treatment
            column_index: Index of the treatment column
            higher_is_better: Whether higher proportion is better

        Returns:
            ProportionSignificanceResult with all test results
        """
        try:
            result = self.proportion_comparison(
                control_successes,
                control_trials,
                treatment_successes,
                treatment_trials,
                higher_is_better
            )
        except Exception as e:
            # Return neutral result on error
            return ProportionSignificanceResult(
                metric_id=metric_id,
                column_index=column_index,
                prob_beat_control=0.5,
                credible_interval_lower=0.0,
                credible_interval_upper=0.0,
                mean_difference=0.0,
                relative_difference=0.0,
                is_significant=False,
                direction="neutral",
                control_mean=0.0,
                treatment_mean=0.0,
                n_control_events=control_trials,
                n_treatment_events=treatment_trials,
                control_successes=control_successes,
                treatment_successes=treatment_successes,
                warning=f"Error computing significance: {str(e)}"
            )

        return ProportionSignificanceResult(
            metric_id=metric_id,
            column_index=column_index,
            prob_beat_control=result['prob_beat_control'],
            credible_interval_lower=result['credible_interval_lower'],
            credible_interval_upper=result['credible_interval_upper'],
            mean_difference=result['mean_difference'],
            relative_difference=result['relative_difference'],
            is_significant=result['is_significant'],
            direction=result['direction'],
            control_mean=result['control_mean'],
            treatment_mean=result['treatment_mean'],
            n_control_events=result['n_control_events'],
            n_treatment_events=result['n_treatment_events'],
            control_successes=result['control_successes'],
            treatment_successes=result['treatment_successes'],
            warning=result.get('warning')
        )
