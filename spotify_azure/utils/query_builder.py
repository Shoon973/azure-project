"""Builds the gold FactStream SQL with Jinja.

Pure Python (no Spark) so it can be unit-tested locally.
"""

from jinja2 import Template

FACT_QUERY_TEMPLATE = Template(
    """
SELECT
    f.*
{%- for d in dims %}
{%- for c in d.columns %},
    {{ d.alias }}.{{ c }} AS {{ d.alias }}_{{ c }}
{%- endfor %}
{%- endfor %}
FROM {{ fact_table }} f
{%- for d in dims %}
LEFT JOIN {{ d.table }} {{ d.alias }}
    ON f.{{ d.fact_key }} = {{ d.alias }}.{{ d.dim_key }}
    AND {{ d.alias }}.__END_AT IS NULL
{%- endfor %}
""".strip()
)


def build_fact_query(fact_table: str, dims: list[dict]) -> str:
    """Return a SELECT joining the fact table to the *current* version of each dimension.

    Each dim dict needs: table, alias, fact_key, dim_key, columns.
    `__END_AT IS NULL` selects the open SCD2 row created by the gold pipeline.
    """
    for d in dims:
        missing = {"table", "alias", "fact_key", "dim_key", "columns"} - d.keys()
        if missing:
            raise ValueError(f"dimension config {d.get('alias', d)} is missing {sorted(missing)}")
    return FACT_QUERY_TEMPLATE.render(fact_table=fact_table, dims=dims)
