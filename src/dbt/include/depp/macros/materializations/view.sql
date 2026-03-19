{#
  The default view materialization uses CREATE VIEW + intermediate rename,
  which breaks when CASCADE drops leave the relation cache stale
  ("already exists" / "does not exist" errors).

  This override uses CREATE OR REPLACE VIEW directly on the target,
  avoiding the intermediate+rename pattern entirely.
#}

{% materialization view, adapter='depp' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}
    create or replace view {{ target_relation }} as (
      {{ sql }}
    );
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
