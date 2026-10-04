//! A value that readers take a snapshot of and writers replace whole.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The current version of a `T`. Readers get an `Arc` of the version they
/// loaded and keep it for as long as they like; a writer replaces the
/// version, and readers of the old one are unaffected. The mutex is held
/// only to clone or swap the `Arc`, or across [`Current::update`]'s closure.
pub struct Current<T>(Mutex<Arc<T>>);

impl<T> Current<T> {
    pub fn new(value: T) -> Self {
        Self::from_arc(Arc::new(value))
    }

    pub fn from_arc(value: Arc<T>) -> Self {
        Self(Mutex::new(value))
    }

    pub fn load(&self) -> Arc<T> {
        Arc::clone(&self.locked())
    }

    pub fn store(&self, value: Arc<T>) {
        *self.locked() = value;
    }

    /// Replace the value with `next(current)`. Concurrent updates run one
    /// at a time, so none is lost.
    pub fn update(&self, next: impl FnOnce(&T) -> T) {
        let mut current = self.locked();
        *current = Arc::new(next(&current));
    }

    /// A panic while the lock was held cannot have torn the `Arc`.
    fn locked(&self) -> MutexGuard<'_, Arc<T>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Current<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("Current")
            .field(&self.load())
            .finish()
    }
}

impl<T: Default> Default for Current<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}
