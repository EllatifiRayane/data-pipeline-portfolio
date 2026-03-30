{% macro normalize_status(column_name) %}

  /*
    Normalize status values into a controlled set of status.

    Input:
      column_name (string) - column containing raw status values

    Logic:
      - LOWER + TRIM for basic normalization
      - REGEXP matching to handle typos / variants
      - Fallback to 'UNKNOWN' for unmapped values

    Output:
      One of: DELIVERED, PENDING, CANCELLED, SHIPPED, UNKNOWN

    Note:
      Regex-based matching is intentionally permissive.
      For strict control, prefer a mapping table (seed + join).
  */

  CASE
    WHEN REGEXP_CONTAINS(LOWER(TRIM({{ column_name }})), r'deliv') THEN 'DELIVERED'
    WHEN REGEXP_CONTAINS(LOWER(TRIM({{ column_name }})), r'pend') THEN 'PENDING'
    WHEN REGEXP_CONTAINS(LOWER(TRIM({{ column_name }})), r'cancel') THEN 'CANCELLED'
    WHEN REGEXP_CONTAINS(LOWER(TRIM({{ column_name }})), r'ship') THEN 'SHIPPED'
    ELSE 'UNKNOWN'
  END

{% endmacro %}

