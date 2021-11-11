{{ config(
    indexes = [{'columns':['_airbyte_active_row','_airbyte_unique_key_scd','_airbyte_emitted_at'],'type': 'btree'}],
    unique_key = "_airbyte_unique_key_scd",
    schema = "tf",
    post_hook = ['drop table if exists _airbyte_tf.effraw_orders_ab3'],
    tags = [ "top-level" ]
) }}
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('effraw_orders_ab3')  }}
    -- effraw_orders from {{ source('tf', '_airbyte_raw_effraw_orders') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at') }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            adapter.quote('id'),
        ]) }} as _airbyte_unique_key
    from new_data
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('effraw_orders_ab3'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro)
    left join {{ ref('effraw_orders_ab3')  }} as inc_data on 1 = 0
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('effraw_orders_ab3')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('effraw_orders_ab3')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('effraw_orders_ab3')  }}
    -- effraw_orders from {{ source('tf', '_airbyte_raw_effraw_orders') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
            adapter.quote('id'),
      ]) }} as _airbyte_unique_key,
        {{ adapter.quote('id') }},
        pid,
        note,
        ctime,
        mtime,
        price,
        status,
        address,
        is_gift,
        user_id,
        ext_data,
        pay_time,
        pay_type,
        s11money,
        coupon_id,
        leave_msg,
        order_num,
        pay_price,
        send_info,
        send_time,
        cancel_time,
        document_id,
        has_push_fb,
        is_feedback,
        refund_info,
        send_status,
        admin_create,
        error_status,
        final_status,
        complete_time,
        has_push_hfdr,
        refund_status,
        transaction_id,
        after_sale_info,
        after_sale_type,
        first_pass_time,
        score_record_id,
        reward_record_id,
        after_sale_status,
        express_sign_time,
      mtime as _airbyte_start_at,
      lag(mtime) over (
        partition by cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }})
        order by
            mtime is null asc,
            mtime desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }})
        order by
            mtime is null asc,
            mtime desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_effraw_orders_hashid
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by _airbyte_unique_key, _airbyte_start_at, _airbyte_emitted_at
            order by _airbyte_ab_id
        ) as _airbyte_row_num,
        {{ dbt_utils.surrogate_key([
          '_airbyte_unique_key',
          '_airbyte_start_at',
          '_airbyte_emitted_at'
        ]) }} as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
        {{ adapter.quote('id') }},
        pid,
        note,
        ctime,
        mtime,
        price,
        status,
        address,
        is_gift,
        user_id,
        ext_data,
        pay_time,
        pay_type,
        s11money,
        coupon_id,
        leave_msg,
        order_num,
        pay_price,
        send_info,
        send_time,
        cancel_time,
        document_id,
        has_push_fb,
        is_feedback,
        refund_info,
        send_status,
        admin_create,
        error_status,
        final_status,
        complete_time,
        has_push_hfdr,
        refund_status,
        transaction_id,
        after_sale_info,
        after_sale_type,
        first_pass_time,
        score_record_id,
        reward_record_id,
        after_sale_status,
        express_sign_time,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_effraw_orders_hashid
from dedup_data where _airbyte_row_num = 1

