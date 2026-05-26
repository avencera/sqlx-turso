/// Result metadata for a Turso query execution
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TursoQueryResult {
    rows_affected: u64,
}

impl TursoQueryResult {
    /// Creates query result metadata
    pub fn new(rows_affected: u64) -> Self {
        Self { rows_affected }
    }

    /// Returns the number of rows affected by the query
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<TursoQueryResult> for TursoQueryResult {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = TursoQueryResult>,
    {
        self.rows_affected += iter
            .into_iter()
            .map(|result| result.rows_affected)
            .sum::<u64>();
    }
}
