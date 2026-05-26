#[test]
fn ui_tests() {
    let offline_dir =
        std::env::temp_dir().join(format!("sqlx-turso-ui-metadata-{}", std::process::id()));
    std::fs::create_dir_all(&offline_dir).expect("failed to create trybuild metadata directory");

    // these compile tests use self-contained live queries instead of cached metadata
    unsafe {
        std::env::set_var("DATABASE_URL", "turso::memory:");
        std::env::set_var("SQLX_OFFLINE_DIR", &offline_dir);
        std::env::set_var("SQLX_OFFLINE", "false");
    }

    let tests = trybuild::TestCases::new();

    tests.pass("tests/ui/pass/*.rs");
    tests.compile_fail("tests/ui/fail/*.rs");
}
