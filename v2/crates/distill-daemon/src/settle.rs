//! Waiting for the filesystem to settle.
//!
//! A burst of changes (a git checkout, a branch switch, an editor's
//! write-rename-chmod) is acted on once it has ended, never part way
//! through: the state in the middle of a burst is one nobody wrote. Each
//! change restarts a trailing quiet window, and the work waits until no
//! change has arrived for the whole window (`watch.quiet_ms`, 250 ms by
//! default).
//!
//! There is no cap. Something that never stops changing holds the window
//! open, and the work waits for it; once a window has been held open for
//! [`HELD_OPEN_WARNING`] the window says so (then every
//! [`HELD_OPEN_REPEAT`] while it stays open), naming the paths that changed
//! most, so they can be moved out of what is watched.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The default quiet window.
pub const DEFAULT_QUIET: Duration = Duration::from_millis(250);
/// The longest quiet window a configuration may ask for.
pub const MAX_QUIET: Duration = Duration::from_secs(5);
/// How long a window stays open before it warns.
pub const HELD_OPEN_WARNING: Duration = Duration::from_secs(5);
/// How often a window that stays open warns again.
pub const HELD_OPEN_REPEAT: Duration = Duration::from_secs(30);
/// How many of the most-changed paths a warning names.
const NAMED_PATHS: usize = 5;
/// How many distinct paths one episode counts; changes to further paths
/// still hold the window open, they are only not named.
const COUNTED_PATHS: usize = 65_536;

/// A trailing quiet window over a stream of changes.
#[derive(Debug)]
pub(crate) struct QuietWindow {
    quiet: Duration,
    /// What waits for the window, for the warning.
    waiting: &'static str,
    episode: Option<Episode>,
}

/// Changes arriving with no quiet window between them.
#[derive(Debug)]
struct Episode {
    since: Instant,
    last: Instant,
    changes: u64,
    paths: HashMap<PathBuf, u64>,
    warned: Option<Instant>,
}

/// A window held open too long: what the warning reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldOpen {
    pub open_for: Duration,
    pub changes: u64,
    /// The most-changed paths, most first, with their change counts.
    pub paths: Vec<(PathBuf, u64)>,
}

impl QuietWindow {
    pub fn new(quiet: Duration, waiting: &'static str) -> Self {
        Self {
            quiet,
            waiting,
            episode: None,
        }
    }

    /// A new window length; it applies to the open window too.
    pub fn set_quiet(&mut self, quiet: Duration) {
        self.quiet = quiet;
    }

    /// A change to `paths` arrived at `now`: the window restarts. Returns
    /// (and logs) the held-open warning when one is due.
    pub fn change<'a>(
        &mut self,
        now: Instant,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Option<HeldOpen> {
        let quiet = self.quiet;
        // A window that already closed (its work has not started yet) ends
        // the episode: the changes were not continuous.
        let episode = match &mut self.episode {
            Some(episode) if now < episode.last + quiet => episode,
            slot => slot.insert(Episode {
                since: now,
                last: now,
                changes: 0,
                paths: HashMap::new(),
                warned: None,
            }),
        };
        episode.last = now;
        episode.changes += 1;
        for path in paths {
            if let Some(count) = episode.paths.get_mut(path) {
                *count += 1;
            } else if episode.paths.len() < COUNTED_PATHS {
                episode.paths.insert(path.to_path_buf(), 1);
            }
        }
        let open_for = now.saturating_duration_since(episode.since);
        let due = match episode.warned {
            None => open_for >= HELD_OPEN_WARNING,
            Some(warned) => now.saturating_duration_since(warned) >= HELD_OPEN_REPEAT,
        };
        if !due {
            return None;
        }
        episode.warned = Some(now);
        let mut paths: Vec<(PathBuf, u64)> = episode
            .paths
            .iter()
            .map(|(path, count)| (path.clone(), *count))
            .collect();
        paths.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        paths.truncate(NAMED_PATHS);
        let held = HeldOpen {
            open_for,
            changes: episode.changes,
            paths,
        };
        tracing::warn!(
            waiting = self.waiting,
            open_for_ms = held.open_for.as_millis() as u64,
            changes = held.changes,
            paths = %held
                .paths
                .iter()
                .map(|(path, count)| format!("{} ({count})", path.display()))
                .collect::<Vec<_>>()
                .join(", "),
            "the filesystem has not settled; the {} waits for it. The paths changing most \
             are named: if they are not inputs, move them out of what is watched",
            self.waiting,
        );
        Some(held)
    }

    /// When the window closes: the last change plus the quiet time; `None`
    /// with no change since the last [`QuietWindow::close`].
    pub fn closes_at(&self) -> Option<Instant> {
        self.episode.as_ref().map(|episode| episode.last + self.quiet)
    }

    /// The waiting work starts: the episode ends.
    pub fn close(&mut self) {
        if let Some(episode) = self.episode.take() {
            if episode.warned.is_some() {
                tracing::info!(
                    waiting = self.waiting,
                    open_for_ms = episode
                        .last
                        .saturating_duration_since(episode.since)
                        .as_millis() as u64,
                    changes = episode.changes,
                    "the filesystem settled",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUIET: Duration = Duration::from_millis(250);

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    #[test]
    fn each_change_restarts_the_window() {
        let start = Instant::now();
        let mut window = QuietWindow::new(QUIET, "test");
        assert_eq!(window.closes_at(), None);
        window.change(start, [Path::new("/a")]);
        assert_eq!(window.closes_at(), Some(start + QUIET));
        window.change(start + ms(200), [Path::new("/a")]);
        assert_eq!(window.closes_at(), Some(start + ms(450)), "trailing, not first-wins");
        window.close();
        assert_eq!(window.closes_at(), None);
    }

    #[test]
    fn a_window_held_open_names_the_noisiest_paths_once_then_rate_limited() {
        let start = Instant::now();
        let mut window = QuietWindow::new(QUIET, "test");
        let noisy = Path::new("/project/assets/log.txt");
        let mut warnings = Vec::new();
        // A noisy path every 100 ms for 40 s; a few others now and then.
        for step in 0..400u64 {
            let now = start + ms(step * 100);
            let mut paths = vec![noisy];
            if step % 50 == 0 {
                paths.push(Path::new("/project/assets/other.png"));
            }
            if let Some(held) = window.change(now, paths) {
                warnings.push((step, held));
            }
        }
        let steps: Vec<u64> = warnings.iter().map(|(step, _)| *step).collect();
        // First at 5 s, then every 30 s.
        assert_eq!(steps, vec![50, 350]);
        let (_, first) = &warnings[0];
        assert_eq!(first.open_for, HELD_OPEN_WARNING);
        assert_eq!(first.changes, 51);
        assert_eq!(first.paths[0], (noisy.to_path_buf(), 51));
        assert_eq!(first.paths[1], (PathBuf::from("/project/assets/other.png"), 2));
    }

    #[test]
    fn a_quiet_gap_starts_a_new_episode() {
        let start = Instant::now();
        let mut window = QuietWindow::new(QUIET, "test");
        let path = Path::new("/a");
        for step in 0..45u64 {
            assert_eq!(window.change(start + ms(step * 100), [path]), None);
        }
        // A gap longer than the window: the work could have run.
        let resumed = start + ms(4400 + 300);
        for step in 0..50u64 {
            assert_eq!(window.change(resumed + ms(step * 100), [path]), None);
        }
        let held = window.change(resumed + ms(5000), [path]).expect("5 s since the gap");
        assert_eq!(held.changes, 51, "only the changes since the gap");
    }
}
