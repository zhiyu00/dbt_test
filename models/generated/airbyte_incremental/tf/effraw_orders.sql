{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "tf",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
select
    _airbyte_unique_key,
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
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_effraw_orders_hashid
from {{ ref('effraw_orders_scd') }}
-- effraw_orders from {{ source('tf', '_airbyte_raw_effraw_orders') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at') }}

