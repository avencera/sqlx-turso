fn main() {
    let _query = sqlx_turso::query_scalar!(
        "SELECT 1 AS \"id!: i64\", 'alice' AS \"name!\""
    );
}
