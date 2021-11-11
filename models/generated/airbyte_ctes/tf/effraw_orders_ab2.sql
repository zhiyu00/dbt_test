{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'hash'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_tf",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    cast(pid as {{ dbt_utils.type_float() }}) as pid,
    cast(note as {{ dbt_utils.type_string() }}) as note,
    cast(ctime as {{ dbt_utils.type_float() }}) as ctime,
    cast(mtime as {{ dbt_utils.type_float() }}) as mtime,
    cast(price as {{ dbt_utils.type_float() }}) as price,
    cast(status as {{ dbt_utils.type_float() }}) as status,
    cast(address as {{ dbt_utils.type_string() }}) as address,
    cast(is_gift as {{ dbt_utils.type_float() }}) as is_gift,
    cast(user_id as {{ dbt_utils.type_float() }}) as user_id,
    cast(ext_data as {{ dbt_utils.type_string() }}) as ext_data,
    cast(pay_time as {{ dbt_utils.type_float() }}) as pay_time,
    {{ cast_to_boolean('pay_type') }} as pay_type,
    cast(s11money as {{ dbt_utils.type_float() }}) as s11money,
    cast(coupon_id as {{ dbt_utils.type_float() }}) as coupon_id,
    cast(leave_msg as {{ dbt_utils.type_string() }}) as leave_msg,
    cast(order_num as {{ dbt_utils.type_string() }}) as order_num,
    cast(pay_price as {{ dbt_utils.type_float() }}) as pay_price,
    cast(send_info as {{ dbt_utils.type_string() }}) as send_info,
    cast(send_time as {{ dbt_utils.type_float() }}) as send_time,
    cast(cancel_time as {{ dbt_utils.type_float() }}) as cancel_time,
    cast(document_id as {{ dbt_utils.type_float() }}) as document_id,
    cast(has_push_fb as {{ dbt_utils.type_float() }}) as has_push_fb,
    cast(is_feedback as {{ dbt_utils.type_float() }}) as is_feedback,
    cast(refund_info as {{ dbt_utils.type_string() }}) as refund_info,
    cast(send_status as {{ dbt_utils.type_float() }}) as send_status,
    cast(admin_create as {{ dbt_utils.type_float() }}) as admin_create,
    cast(error_status as {{ dbt_utils.type_float() }}) as error_status,
    cast(final_status as {{ dbt_utils.type_float() }}) as final_status,
    cast(complete_time as {{ dbt_utils.type_float() }}) as complete_time,
    cast(has_push_hfdr as {{ dbt_utils.type_float() }}) as has_push_hfdr,
    cast(refund_status as {{ dbt_utils.type_float() }}) as refund_status,
    cast(transaction_id as {{ dbt_utils.type_string() }}) as transaction_id,
    cast(after_sale_info as {{ dbt_utils.type_string() }}) as after_sale_info,
    cast(after_sale_type as {{ dbt_utils.type_float() }}) as after_sale_type,
    cast(first_pass_time as {{ dbt_utils.type_float() }}) as first_pass_time,
    cast(score_record_id as {{ dbt_utils.type_float() }}) as score_record_id,
    cast(reward_record_id as {{ dbt_utils.type_float() }}) as reward_record_id,
    cast(after_sale_status as {{ dbt_utils.type_float() }}) as after_sale_status,
    cast(express_sign_time as {{ dbt_utils.type_float() }}) as express_sign_time,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('effraw_orders_ab1') }}
-- effraw_orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

