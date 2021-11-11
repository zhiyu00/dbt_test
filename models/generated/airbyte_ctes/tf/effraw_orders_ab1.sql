{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'hash'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_tf",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['pid'], ['pid']) }} as pid,
    {{ json_extract_scalar('_airbyte_data', ['note'], ['note']) }} as note,
    {{ json_extract_scalar('_airbyte_data', ['ctime'], ['ctime']) }} as ctime,
    {{ json_extract_scalar('_airbyte_data', ['mtime'], ['mtime']) }} as mtime,
    {{ json_extract_scalar('_airbyte_data', ['price'], ['price']) }} as price,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['address'], ['address']) }} as address,
    {{ json_extract_scalar('_airbyte_data', ['is_gift'], ['is_gift']) }} as is_gift,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    {{ json_extract_scalar('_airbyte_data', ['ext_data'], ['ext_data']) }} as ext_data,
    {{ json_extract_scalar('_airbyte_data', ['pay_time'], ['pay_time']) }} as pay_time,
    {{ json_extract_scalar('_airbyte_data', ['pay_type'], ['pay_type']) }} as pay_type,
    {{ json_extract_scalar('_airbyte_data', ['s11money'], ['s11money']) }} as s11money,
    {{ json_extract_scalar('_airbyte_data', ['coupon_id'], ['coupon_id']) }} as coupon_id,
    {{ json_extract_scalar('_airbyte_data', ['leave_msg'], ['leave_msg']) }} as leave_msg,
    {{ json_extract_scalar('_airbyte_data', ['order_num'], ['order_num']) }} as order_num,
    {{ json_extract_scalar('_airbyte_data', ['pay_price'], ['pay_price']) }} as pay_price,
    {{ json_extract_scalar('_airbyte_data', ['send_info'], ['send_info']) }} as send_info,
    {{ json_extract_scalar('_airbyte_data', ['send_time'], ['send_time']) }} as send_time,
    {{ json_extract_scalar('_airbyte_data', ['cancel_time'], ['cancel_time']) }} as cancel_time,
    {{ json_extract_scalar('_airbyte_data', ['document_id'], ['document_id']) }} as document_id,
    {{ json_extract_scalar('_airbyte_data', ['has_push_fb'], ['has_push_fb']) }} as has_push_fb,
    {{ json_extract_scalar('_airbyte_data', ['is_feedback'], ['is_feedback']) }} as is_feedback,
    {{ json_extract_scalar('_airbyte_data', ['refund_info'], ['refund_info']) }} as refund_info,
    {{ json_extract_scalar('_airbyte_data', ['send_status'], ['send_status']) }} as send_status,
    {{ json_extract_scalar('_airbyte_data', ['admin_create'], ['admin_create']) }} as admin_create,
    {{ json_extract_scalar('_airbyte_data', ['error_status'], ['error_status']) }} as error_status,
    {{ json_extract_scalar('_airbyte_data', ['final_status'], ['final_status']) }} as final_status,
    {{ json_extract_scalar('_airbyte_data', ['complete_time'], ['complete_time']) }} as complete_time,
    {{ json_extract_scalar('_airbyte_data', ['has_push_hfdr'], ['has_push_hfdr']) }} as has_push_hfdr,
    {{ json_extract_scalar('_airbyte_data', ['refund_status'], ['refund_status']) }} as refund_status,
    {{ json_extract_scalar('_airbyte_data', ['transaction_id'], ['transaction_id']) }} as transaction_id,
    {{ json_extract_scalar('_airbyte_data', ['after_sale_info'], ['after_sale_info']) }} as after_sale_info,
    {{ json_extract_scalar('_airbyte_data', ['after_sale_type'], ['after_sale_type']) }} as after_sale_type,
    {{ json_extract_scalar('_airbyte_data', ['first_pass_time'], ['first_pass_time']) }} as first_pass_time,
    {{ json_extract_scalar('_airbyte_data', ['score_record_id'], ['score_record_id']) }} as score_record_id,
    {{ json_extract_scalar('_airbyte_data', ['reward_record_id'], ['reward_record_id']) }} as reward_record_id,
    {{ json_extract_scalar('_airbyte_data', ['after_sale_status'], ['after_sale_status']) }} as after_sale_status,
    {{ json_extract_scalar('_airbyte_data', ['express_sign_time'], ['express_sign_time']) }} as express_sign_time,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('tf', '_airbyte_raw_effraw_orders') }} as table_alias
-- effraw_orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

