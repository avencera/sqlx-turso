fn main() {
    let _query = sqlx_turso::query!(
        "SELECT ? AS \"value!: i64\"",
        1_i64,
        2_i64
    );
}
