use std::path::{Path, PathBuf};

pub(crate) fn known_database_files(path: &Path) -> Vec<PathBuf> {
    let base = path.as_os_str().to_string_lossy();

    [
        "",
        "-wal",
        "-shm",
        "-wal-tshm",
        "-wal-revert",
        "-info",
        "-changes",
    ]
    .into_iter()
    .map(|suffix| PathBuf::from(format!("{base}{suffix}")))
    .collect()
}
