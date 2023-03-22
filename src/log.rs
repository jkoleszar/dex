use std::fmt::Display;

pub trait LogIfError<T, E: Display>: Into<Result<T, E>> {
    fn log_if_error(self, context: &str) -> Result<T, E> {
        let result = self.into();
        if let Err(e) = &result {
            log::error!("{context}: {e}");
        }
        result
    }
}
impl<T, E: Display> LogIfError<T, E> for Result<T, E> {}
