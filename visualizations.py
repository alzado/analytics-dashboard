import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from typing import List, Dict


def create_kpi_card_html(title: str, value: str, delta: str = None, delta_color: str = "green") -> str:
    """Create HTML for KPI card."""
    delta_html = ""
    if delta:
        color = delta_color if delta_color else ("green" if "+" in delta else "red")
        delta_html = f'<p style="color: {color}; font-size: 14px; margin: 5px 0 0 0;">{delta}</p>'

    return f"""
    <div style="padding: 20px; background-color: #f8f9fa; border-radius: 8px; border-left: 4px solid #1f77b4;">
        <h4 style="margin: 0; color: #666; font-size: 14px;">{title}</h4>
        <h2 style="margin: 10px 0 0 0; color: #333;">{value}</h2>
        {delta_html}
    </div>
    """


def create_trend_chart(
    df: pd.DataFrame,
    metric: str,
    title: str,
    yaxis_title: str = None,
    comparison_df: pd.DataFrame = None
) -> go.Figure:
    """Create time series trend chart."""
    fig = go.Figure()

    # Check if metric is a rate (should be shown as percentage)
    rate_metrics = ['ctr', 'a2c_rate', 'conversion_rate']
    is_rate = metric in rate_metrics

    # Convert to percentage if it's a rate metric
    y_values = df[metric] * 100 if is_rate else df[metric]

    # Format hover text
    if is_rate:
        hover_template = '%{y:.2f}%<extra></extra>'
    elif 'revenue' in metric or 'purchase' in metric:
        hover_template = '$%{y:.2f}<extra></extra>'
    else:
        hover_template = '%{y:,.0f}<extra></extra>'

    # Main period
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=y_values,
        mode='lines+markers',
        name='Current Period',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=6),
        hovertemplate=hover_template
    ))

    # Comparison period if provided
    if comparison_df is not None:
        y_values_comp = comparison_df[metric] * 100 if is_rate else comparison_df[metric]
        fig.add_trace(go.Scatter(
            x=comparison_df['date'],
            y=y_values_comp,
            mode='lines+markers',
            name='Comparison Period',
            line=dict(color='#ff7f0e', width=2, dash='dash'),
            marker=dict(size=6),
            hovertemplate=hover_template
        ))

    # Update y-axis title to include unit
    if yaxis_title is None:
        yaxis_title = metric.replace('_', ' ').title()

    if is_rate:
        yaxis_title = f"{yaxis_title} (%)"

    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        template='plotly_white',
        height=400
    )

    return fig


def create_multi_metric_trend(
    df: pd.DataFrame,
    metrics: List[str],
    metric_names: List[str],
    title: str
) -> go.Figure:
    """Create chart with multiple metrics on subplots."""
    n_metrics = len(metrics)
    fig = make_subplots(
        rows=n_metrics,
        cols=1,
        subplot_titles=metric_names,
        vertical_spacing=0.08
    )

    colors = px.colors.qualitative.Plotly

    for i, (metric, name) in enumerate(zip(metrics, metric_names), 1):
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df[metric],
                mode='lines+markers',
                name=name,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=5),
                showlegend=False
            ),
            row=i,
            col=1
        )

    fig.update_layout(
        title_text=title,
        height=300 * n_metrics,
        template='plotly_white'
    )

    fig.update_xaxes(title_text="Date", row=n_metrics, col=1)

    return fig


def create_channel_comparison(df: pd.DataFrame) -> go.Figure:
    """Create comparison chart for channel performance."""
    channel_data = df.groupby('channel').agg({
        'queries': 'sum',
        'queries_pdp': 'sum',
        'queries_a2c': 'sum',
        'purchases': 'sum',
        'gross_purchase': 'sum'
    }).reset_index()

    # Calculate rates
    channel_data['ctr'] = channel_data['queries_pdp'] / channel_data['queries']
    channel_data['a2c_rate'] = channel_data['queries_a2c'] / channel_data['queries']
    channel_data['conversion_rate'] = channel_data['purchases'] / channel_data['queries']
    channel_data['revenue_per_query'] = channel_data['gross_purchase'] / channel_data['queries']

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=['Click-Through Rate', 'Add-to-Cart Rate', 'Conversion Rate', 'Revenue per Query'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'bar'}]]
    )

    metrics = [
        ('ctr', 1, 1, 'CTR', True),
        ('a2c_rate', 1, 2, 'A2C Rate', True),
        ('conversion_rate', 2, 1, 'Conv. Rate', True),
        ('revenue_per_query', 2, 2, 'Rev/Query', False)
    ]

    for metric, row, col, label, is_rate in metrics:
        if is_rate:
            # Convert to percentage and format text
            y_values = channel_data[metric] * 100
            text_values = [f"{val:.2f}%" for val in y_values]
        else:
            # Keep as currency
            y_values = channel_data[metric]
            text_values = [f"${val:.2f}" for val in y_values]

        fig.add_trace(
            go.Bar(
                x=channel_data['channel'],
                y=y_values,
                name=label,
                text=text_values,
                textposition='auto',
                showlegend=False
            ),
            row=row,
            col=col
        )

    fig.update_layout(
        title_text="Channel Performance Comparison",
        height=600,
        template='plotly_white'
    )

    return fig


def create_attribute_performance_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart for attribute performance."""
    if df.empty:
        return go.Figure()

    # Calculate percentage of total queries
    total_queries = df['queries'].sum()
    df = df.copy()
    df['query_pct'] = (df['queries'] / total_queries) * 100

    # Sort by query percentage
    df = df.sort_values('query_pct', ascending=True)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=['% of Total Queries by Attribute', 'Revenue by Attribute'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Query percentage
    fig.add_trace(
        go.Bar(
            y=df['attribute'],
            x=df['query_pct'],
            orientation='h',
            name='% Queries',
            text=[f"{val:.1f}%" for val in df['query_pct']],
            textposition='auto',
            marker_color='#1f77b4',
            showlegend=False
        ),
        row=1,
        col=1
    )

    # Revenue
    fig.add_trace(
        go.Bar(
            y=df['attribute'],
            x=df['revenue'],
            orientation='h',
            name='Revenue',
            text=[f"${val:,.2f}" for val in df['revenue']],
            textposition='auto',
            marker_color='#2ca02c',
            showlegend=False
        ),
        row=1,
        col=2
    )

    fig.update_layout(
        title_text="Performance by Attribute",
        height=max(400, len(df) * 40),
        template='plotly_white'
    )

    return fig


def create_attribute_combination_chart(df: pd.DataFrame) -> go.Figure:
    """Create horizontal bar chart for top attribute combinations."""
    if df.empty:
        return go.Figure()

    # Calculate percentage of total queries
    total_queries = df['queries'].sum()
    df = df.copy()
    df['query_pct'] = (df['queries'] / total_queries) * 100

    # Sort by query percentage (already sorted by queries, but ensure it)
    df = df.sort_values('query_pct', ascending=True)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=['% of Total Queries by Attribute Combination', 'Conversion Rate by Combination'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Query percentage
    fig.add_trace(
        go.Bar(
            y=df['attribute_combination'],
            x=df['query_pct'],
            orientation='h',
            name='% Queries',
            text=[f"{val:.1f}%" for val in df['query_pct']],
            textposition='auto',
            marker_color='#1f77b4',
            showlegend=False
        ),
        row=1,
        col=1
    )

    # Conversion Rate
    fig.add_trace(
        go.Bar(
            y=df['attribute_combination'],
            x=df['conversion_rate'] * 100,
            orientation='h',
            name='Conversion Rate',
            text=[f"{val:.2f}%" for val in df['conversion_rate'] * 100],
            textposition='auto',
            marker_color='#d62728',
            showlegend=False
        ),
        row=1,
        col=2
    )

    fig.update_layout(
        title_text="Performance by Attribute Combination",
        height=max(500, len(df) * 35),
        template='plotly_white'
    )

    fig.update_xaxes(title_text="% of Queries", row=1, col=1)
    fig.update_xaxes(title_text="Conversion Rate (%)", row=1, col=2)

    return fig


def create_n_attributes_chart(df: pd.DataFrame) -> go.Figure:
    """Create chart showing performance by number of attributes."""
    attr_data = df.groupby('n_attributes').agg({
        'queries': 'sum',
        'queries_pdp': 'sum',
        'queries_a2c': 'sum',
        'purchases': 'sum',
        'gross_purchase': 'sum'
    }).reset_index()

    # Calculate rates and percentages
    total_queries = attr_data['queries'].sum()
    attr_data['query_pct'] = (attr_data['queries'] / total_queries) * 100
    attr_data['ctr'] = attr_data['queries_pdp'] / attr_data['queries']
    attr_data['conversion_rate'] = attr_data['purchases'] / attr_data['queries']

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=['% of Total Queries by # Attributes', 'Conversion Rate by # Attributes'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Query percentage
    fig.add_trace(
        go.Bar(
            x=attr_data['n_attributes'],
            y=attr_data['query_pct'],
            name='% Queries',
            text=[f"{val:.1f}%" for val in attr_data['query_pct']],
            textposition='auto',
            marker_color='#1f77b4',
            showlegend=False
        ),
        row=1,
        col=1
    )

    # Conversion Rate
    fig.add_trace(
        go.Bar(
            x=attr_data['n_attributes'],
            y=attr_data['conversion_rate'] * 100,
            name='Conversion Rate',
            text=[f"{val:.2f}%" for val in attr_data['conversion_rate'] * 100],
            textposition='auto',
            marker_color='#d62728',
            showlegend=False
        ),
        row=1,
        col=2
    )

    fig.update_layout(
        title_text="Performance by Number of Attributes",
        height=400,
        template='plotly_white'
    )

    fig.update_xaxes(title_text="Number of Attributes", row=1, col=1)
    fig.update_xaxes(title_text="Number of Attributes", row=1, col=2)

    return fig


def create_n_words_chart(words_data: pd.DataFrame, word_col: str = 'n_words_grouped') -> go.Figure:
    """
    Create chart showing performance by number of words in search query.

    Args:
        words_data: Pre-aggregated dataframe with word count metrics
        word_col: Name of the column containing word counts (e.g., 'n_words_grouped', 'n_words_normalized')
    """
    # Calculate percentage of total queries
    total_queries = words_data['queries'].sum()
    words_data['query_pct'] = (words_data['queries'] / total_queries) * 100

    # Create subplots - 2x2 grid
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=['% of Total Queries by # Words', 'CTR by # Words', 'Conversion Rate by # Words', 'Revenue/Query by # Words'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Query percentage
    fig.add_trace(
        go.Bar(
            x=words_data[word_col],
            y=words_data['query_pct'],
            name='% Queries',
            text=[f"{val:.1f}%" for val in words_data['query_pct']],
            textposition='auto',
            marker_color='#1f77b4',
            showlegend=False
        ),
        row=1,
        col=1
    )

    # CTR
    fig.add_trace(
        go.Bar(
            x=words_data[word_col],
            y=words_data['ctr'] * 100,
            name='CTR',
            text=[f"{val:.2f}%" for val in words_data['ctr'] * 100],
            textposition='auto',
            marker_color='#ff7f0e',
            showlegend=False
        ),
        row=1,
        col=2
    )

    # Conversion Rate
    fig.add_trace(
        go.Bar(
            x=words_data[word_col],
            y=words_data['conversion_rate'] * 100,
            name='Conversion Rate',
            text=[f"{val:.2f}%" for val in words_data['conversion_rate'] * 100],
            textposition='auto',
            marker_color='#d62728',
            showlegend=False
        ),
        row=2,
        col=1
    )

    # Revenue per Query
    fig.add_trace(
        go.Bar(
            x=words_data[word_col],
            y=words_data['revenue_per_query'],
            name='Revenue/Query',
            text=[f"${val:.2f}" for val in words_data['revenue_per_query']],
            textposition='auto',
            marker_color='#2ca02c',
            showlegend=False
        ),
        row=2,
        col=2
    )

    fig.update_layout(
        title_text="Performance by Number of Words in Search Query",
        height=700,
        template='plotly_white'
    )

    fig.update_xaxes(title_text="Number of Words", row=1, col=1)
    fig.update_xaxes(title_text="Number of Words", row=1, col=2)
    fig.update_xaxes(title_text="Number of Words", row=2, col=1)
    fig.update_xaxes(title_text="Number of Words", row=2, col=2)

    return fig


def create_top_searches_chart(df: pd.DataFrame, metric: str = 'queries', top_n: int = 20) -> go.Figure:
    """Create horizontal bar chart of top search terms."""
    df_top = df.nlargest(top_n, metric).sort_values(metric, ascending=True)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df_top['search_term'],
        x=df_top[metric],
        orientation='h',
        text=df_top[metric].round(2),
        textposition='auto',
        marker_color='#1f77b4'
    ))

    fig.update_layout(
        title=f"Top {top_n} Search Terms by {metric.replace('_', ' ').title()}",
        xaxis_title=metric.replace('_', ' ').title(),
        yaxis_title='Search Term',
        height=max(400, top_n * 25),
        template='plotly_white'
    )

    return fig


def format_metric_value(value: float, metric_type: str) -> str:
    """Format metric values for display."""
    if metric_type == 'currency':
        return f"${value:,.2f}"
    elif metric_type == 'percentage':
        return f"{value * 100:.2f}%"
    elif metric_type == 'integer':
        return f"{int(value):,}"
    else:
        return f"{value:,.2f}"


def create_comparison_table(comparison_data: Dict) -> pd.DataFrame:
    """Create formatted comparison table for two periods."""
    rows = []
    for metric, values in comparison_data.items():
        rows.append({
            'Metric': metric.replace('_', ' ').title(),
            'Period 1': format_metric_value(values['period1'], 'currency' if 'revenue' in metric else 'integer'),
            'Period 2': format_metric_value(values['period2'], 'currency' if 'revenue' in metric else 'integer'),
            'Change': format_metric_value(values['change'], 'currency' if 'revenue' in metric else 'integer'),
            '% Change': f"{values['pct_change']:.2f}%"
        })
    return pd.DataFrame(rows)


def create_crosstab_heatmap(
    pivot_df: pd.DataFrame,
    row_dimension_name: str,
    col_dimension_name: str,
    metric_name: str,
    metric_format: str
) -> go.Figure:
    """
    Create an interactive heatmap for cross-tab analysis.

    Args:
        pivot_df: Pivot table with metric values
        row_dimension_name: Display name for row dimension
        col_dimension_name: Display name for column dimension
        metric_name: Display name for metric
        metric_format: Format type ('percent', 'currency', 'number')

    Returns:
        Plotly Figure with heatmap
    """
    # Prepare data for heatmap
    z_data = pivot_df.values
    x_labels = pivot_df.columns.tolist()
    y_labels = pivot_df.index.tolist()

    # Format text annotations based on metric format
    text_data = []
    for row in z_data:
        text_row = []
        for val in row:
            if pd.isna(val):
                text_row.append('')
            elif metric_format == 'percent':
                text_row.append(f'{val * 100:.1f}%')
            elif metric_format == 'currency':
                text_row.append(f'${val:.2f}')
            else:
                text_row.append(f'{val:,.0f}')
        text_data.append(text_row)

    # Create hover text
    hover_data = []
    for i, row_label in enumerate(y_labels):
        hover_row = []
        for j, col_label in enumerate(x_labels):
            val = z_data[i][j]
            if pd.isna(val):
                hover_row.append('')
            else:
                if metric_format == 'percent':
                    val_str = f'{val * 100:.2f}%'
                elif metric_format == 'currency':
                    val_str = f'${val:.2f}'
                else:
                    val_str = f'{val:,.0f}'
                hover_row.append(
                    f'<b>{row_dimension_name}:</b> {row_label}<br>' +
                    f'<b>{col_dimension_name}:</b> {col_label}<br>' +
                    f'<b>{metric_name}:</b> {val_str}'
                )
        hover_data.append(hover_row)

    # Choose colorscale based on metric type
    if metric_format == 'percent':
        colorscale = 'RdYlGn'  # Red-Yellow-Green for rates
    elif metric_format == 'currency':
        colorscale = 'Blues'  # Blues for revenue
    else:
        colorscale = 'Viridis'  # Viridis for counts

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=x_labels,
        y=y_labels,
        text=text_data,
        hovertext=hover_data,
        hoverinfo='text',
        texttemplate='%{text}',
        textfont={"size": 10},
        colorscale=colorscale,
        showscale=True,
        colorbar=dict(
            title=metric_name,
            tickformat='.1%' if metric_format == 'percent' else ('$,.0f' if metric_format == 'currency' else ',.0f')
        )
    ))

    # Update layout
    fig.update_layout(
        title=f'{metric_name} by {row_dimension_name} × {col_dimension_name}',
        xaxis_title=col_dimension_name,
        yaxis_title=row_dimension_name,
        height=max(400, len(y_labels) * 30 + 100),
        template='plotly_white',
        xaxis={'side': 'bottom'},
        yaxis={'autorange': 'reversed'}  # Top to bottom
    )

    return fig
