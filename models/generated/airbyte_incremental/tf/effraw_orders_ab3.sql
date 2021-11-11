{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'hash'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_tf",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        'pid',
        'note',
        'ctime',
        'mtime',
        'price',
        'status',
        'address',
        'is_gift',
        'user_id',
        'ext_data',
        'pay_time',
        boolean_to_string('pay_type'),
        's11money',
        'coupon_id',
        'leave_msg',
        'order_num',
        'pay_price',
        'send_info',
        'send_time',
        'cancel_time',
        'document_id',
        'has_push_fb',
        'is_feedback',
        'refund_info',
        'send_status',
        'admin_create',
        'error_status',
        'final_status',
        'complete_time',
        'has_push_hfdr',
        'refund_status',
        'transaction_id',
        'after_sale_info',
        'after_sale_type',
        'first_pass_time',
        'score_record_id',
        'reward_record_id',
        'after_sale_status',
        'express_sign_time',
    ]) }} as _airbyte_effraw_orders_hashid,
    tmp.*
from {{ ref('effraw_orders_ab2') }} tmp
-- effraw_orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

