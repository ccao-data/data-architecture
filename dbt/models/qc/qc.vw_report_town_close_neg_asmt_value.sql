{% set val_columns = [
    'val01', 'val02', 'val03', 'val04', 'val05', 'val06', 'val07', 'val08', 'val09', 'val10',
    'val11', 'val12', 'val13', 'val16', 'val17', 'val18', 'val19', 'val20', 'val21', 'val22', 'val23',
    'val24', 'val25', 'val26', 'val27', 'val28', 'val29', 'val30', 'val31', 'val32', 'val33', 'val34',
    'val35', 'val36', 'val37', 'val38', 'val39', 'val40', 'val41', 'val42', 'val43', 'val44', 'val45',
    'val46', 'val47', 'val49', 'val50', 'val51', 'val52', 'val53', 'val54', 'val55', 'val56', 'val57',
    'val58', 'val59', 'val60', 'valapr1', 'valapr2', 'valapr3', 'valasm1', 'valasm2', 'valasm3'
] %}

SELECT
    parid,
    taxyr,
    township_code,
    class,
    valclass,
{% for column in val_columns %}
    {{ column }}{% if not loop.last %},{% endif %}
{% endfor %}
FROM {{ ref('qc.vw_neg_asmt_value') }}
WHERE
{% for column in val_columns %}
    {{ column }} < 0{% if not loop.last %} OR{% endif %}
{% endfor %}
