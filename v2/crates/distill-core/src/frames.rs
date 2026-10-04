//! Expansion frames and back-reference resolution (§5).
//!
//! A schema walk opens one frame per struct/enum node it expands (a variant
//! payload opens none: its enum's frame covers it). `BackRef(d)` names the
//! frame `d` levels up the open stack, 0 = innermost. Projection emits one
//! when expansion meets a type already open, so the target was expanded
//! with exactly the frames above it open — and re-entering it must restore
//! that stack. Frames opened between the target and the back-reference are
//! not the target's ancestors: a back-reference inside the re-entered
//! target that counted through them would name the wrong type.
//!
//! Every walker resolves back-references through this module, whichever
//! node type its frames hold (logical schema nodes, wire nodes).

/// The frame `BackRef(distance)` names in `open` (innermost last), and the
/// frames open above it: the stack to re-enter it under. `None`: the
/// distance escapes the open frames.
pub fn resolve_backref<T>(open: &[T], distance: u32) -> Option<(&T, &[T])> {
    let index = backref_index(open.len(), distance)?;
    Some((&open[index], &open[..index]))
}

/// Re-enter the frame `BackRef(distance)` names in a walker's own frame
/// stack: cut `open` back to the target's ancestors and return the target
/// with the [`Reentry`] that puts the cut frames back once the walk of the
/// target returns. `None`: the distance escapes the open frames.
pub fn reenter<T: Copy>(open: &mut Vec<T>, distance: u32) -> Option<(T, Reentry<T>)> {
    let index = backref_index(open.len(), distance)?;
    let cut = open.split_off(index);
    Some((
        cut[0],
        Reentry {
            depth: index,
            frames: cut,
        },
    ))
}

/// The frames [`reenter`] cut from a stack.
#[must_use = "restore the cut frames once the re-entered walk returns"]
pub struct Reentry<T> {
    depth: usize,
    frames: Vec<T>,
}

impl<T> Reentry<T> {
    /// Put the cut frames back, dropping whatever the re-entered walk left
    /// open (an error return may skip its pops).
    pub fn restore(self, open: &mut Vec<T>) {
        open.truncate(self.depth);
        open.extend(self.frames);
    }
}

fn backref_index(open: usize, distance: u32) -> Option<usize> {
    open.checked_sub(1)?
        .checked_sub(usize::try_from(distance).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backref_resolves_to_its_target_under_the_targets_ancestors() {
        let open = ['a', 'b', 'c'];
        assert_eq!(resolve_backref(&open, 0), Some((&'c', &open[..2])));
        assert_eq!(resolve_backref(&open, 2), Some((&'a', &open[..0])));
        assert_eq!(resolve_backref(&open, 3), None);
        assert_eq!(resolve_backref::<char>(&[], 0), None);
    }

    #[test]
    fn reentry_cuts_to_the_ancestors_and_restores_the_stack() {
        let mut open = vec!['a', 'b', 'c'];
        let (target, reentry) = reenter(&mut open, 1).unwrap();
        assert_eq!((target, &open[..]), ('b', &['a'][..]));
        open.extend(['b', 'x']);
        reentry.restore(&mut open);
        assert_eq!(open, ['a', 'b', 'c']);
        assert!(reenter(&mut open, 3).is_none());
        assert_eq!(open, ['a', 'b', 'c']);
    }
}
