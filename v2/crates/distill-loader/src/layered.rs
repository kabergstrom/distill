//! A stack of `LoaderIO` layers seen by the loader as one (DESIGN.md §16,
//! "Layer stack"; newgameplus GAP 28 §7.1).
//!
//! - Layers are ordered bottom to top, e.g. an embedded engine pack, a game
//!   pack, the daemon's `RpcIo`. Lookup is first hit from the top: layers
//!   add or replace, never delete.
//! - A resolve (UUID or path) goes to every layer under that layer's basis;
//!   the answer is the topmost layer naming it, taken once every higher
//!   layer has answered "not named" (`Missing`, `Deleted`, a path's
//!   `Unsupported`) or is unavailable.
//! - An optional layer that is not bound (never connected, or its
//!   connection lost) is unavailable: its "not named" answer means nothing.
//!   A request no available layer names stays unanswered while a layer above
//!   is unavailable; once that layer binds it is asked again, and the
//!   top-down rule applies to its answer. What the unavailable layer named
//!   before fails (`RpcIo` answers `RequestError`), so resident values stay.
//! - A fetch goes to the layer whose resolve answered that content hash,
//!   under the layer basis that resolve was asked at.
//! - The stack's basis is `IoBasis::Layers`, one basis per layer; a delta
//!   in any layer moves the loader to a new round. A layer's basis is ended
//!   at the layer only once no live stack basis shares it.
//! - Each request gets exactly one terminal event, under the stack basis it
//!   was issued with; `end_sweep` and `bind_target` cancel without one.
//! - Targets: each layer binds its own target (`Layer::with_target`, e.g.
//!   an engine pack built for the engine's target). The stack is bound once
//!   every layer not marked `optional` is.
//! - An optional layer (the daemon) neither delays the binding nor fails
//!   it. Losing its connection costs no re-resolve and no deletion: the
//!   stack rebinds it alone. When it binds after the stack did (the daemon
//!   connects), it may name what a lower layer answered: every subscribed
//!   asset and path is published as changed, so each re-resolves and a
//!   newly named one becomes a new revision.

use std::any::Any;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use distill_core::id::{AssetUuid, ContentHash};
use distill_store::state::{InputVersion, SnapshotStamp, StoreInstanceId};

use crate::basis::IoBasis;
use crate::io::{
    AssetDeltaState, AssetPath, IoEvent, LoaderIO, PathResolveResult, ReqId, ResolveResult,
    RuntimeTarget,
};

/// One layer of a [`LayeredIO`].
pub struct Layer {
    io: Box<dyn LayerIo>,
    target_definition_hash: Option<[u8; 16]>,
    optional: bool,
}

impl Layer {
    pub fn new(io: impl LoaderIO + 'static) -> Self {
        Self {
            io: Box::new(io),
            target_definition_hash: None,
            optional: false,
        }
    }

    /// Bind this layer to its own target definition instead of the
    /// loader's (an engine pack under a game).
    pub fn with_target(mut self, target_definition_hash: [u8; 16]) -> Self {
        self.target_definition_hash = Some(target_definition_hash);
        self
    }

    /// The stack binds without waiting for this layer; its rejections are
    /// diagnostics, a lost connection is rebound inside the stack, and
    /// while unbound it is unavailable (module docs).
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
}

/// `LoaderIO` with downcasting, so the engine reaches a layer's own API
/// (`RpcIo::status`, authoring calls).
trait LayerIo: LoaderIO {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: LoaderIO + 'static> LayerIo for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct LayerState {
    layer: Layer,
    bound: bool,
}

/// One layer's part of a resolve.
#[derive(Clone)]
enum Answer {
    Waiting,
    Got(IoEvent),
    /// "Not named" from an optional layer that is not bound: asked again
    /// once it binds.
    Unavailable,
}

#[derive(Clone)]
struct Slot {
    answer: Answer,
    /// The layer basis the layer was asked at.
    basis: IoBasis,
}

enum Asked {
    Asset(AssetUuid),
    Path(AssetPath),
}

enum Pending {
    Resolve {
        asked: Asked,
        basis: IoBasis,
        slots: Vec<Slot>,
    },
    Fetch {
        basis: IoBasis,
        layer: usize,
    },
}

/// A composite [`LoaderIO`] over ordered layers (module docs).
pub struct LayeredIO {
    layers: Vec<LayerState>,
    target: Option<RuntimeTarget>,
    /// `TargetBound` was published for `target`.
    bound: bool,
    /// Stack bases begun and not yet ended.
    live: Vec<IoBasis>,
    last_basis: IoBasis,
    pending: HashMap<ReqId, Pending>,
    /// Per live stack basis: the layer that answered each content hash, and
    /// the layer basis it answered at.
    routes: Vec<(IoBasis, HashMap<ContentHash, (usize, IoBasis)>)>,
    /// Layer bases a request was asked again at (stack basis, layer, layer
    /// basis), ended with the stack basis.
    reasked: Vec<(IoBasis, usize, IoBasis)>,
    assets: BTreeSet<AssetUuid>,
    paths: BTreeSet<String>,
    events: VecDeque<IoEvent>,
}

impl LayeredIO {
    /// A stack of `layers`, bottom to top.
    pub fn new(layers: Vec<Layer>) -> Self {
        assert!(!layers.is_empty(), "a layer stack needs a layer");
        Self {
            layers: layers
                .into_iter()
                .map(|layer| LayerState {
                    layer,
                    bound: false,
                })
                .collect(),
            target: None,
            bound: false,
            live: Vec::new(),
            last_basis: IoBasis::Layers(Arc::from(Vec::new())),
            pending: HashMap::new(),
            routes: Vec::new(),
            reasked: Vec::new(),
            assets: BTreeSet::new(),
            paths: BTreeSet::new(),
            events: VecDeque::new(),
        }
    }

    /// The topmost layer of type `T`.
    pub fn layer<T: LoaderIO + 'static>(&self) -> Option<&T> {
        self.layers
            .iter()
            .rev()
            .find_map(|state| state.layer.io.as_any().downcast_ref())
    }

    /// The topmost layer of type `T`.
    pub fn layer_mut<T: LoaderIO + 'static>(&mut self) -> Option<&mut T> {
        self.layers
            .iter_mut()
            .rev()
            .find_map(|state| state.layer.io.as_any_mut().downcast_mut())
    }

    /// Whether layer `index` (bottom 0) is bound to its target.
    pub fn layer_bound(&self, index: usize) -> bool {
        self.layers.get(index).is_some_and(|state| state.bound)
    }

    fn unavailable(&self, index: usize) -> bool {
        let state = &self.layers[index];
        state.layer.optional && !state.bound
    }

    fn layer_target(&self, index: usize) -> Option<RuntimeTarget> {
        let target = self.target.as_ref()?;
        Some(RuntimeTarget {
            epoch: target.epoch,
            target_definition_hash: self.layers[index]
                .layer
                .target_definition_hash
                .unwrap_or(target.target_definition_hash),
        })
    }

    fn layer_bases<'a>(&self, basis: &'a IoBasis) -> Option<&'a [IoBasis]> {
        basis.layers().filter(|bases| bases.len() == self.layers.len())
    }

    fn foreign_basis(&mut self, req: ReqId, basis: &IoBasis) {
        self.events.push_back(IoEvent::RequestError {
            req,
            message: "not a basis of this layer stack".into(),
            basis: basis.clone(),
        });
    }

    fn publish_bound(&mut self) {
        if self.bound
            || !self
                .layers
                .iter()
                .all(|state| state.bound || state.layer.optional)
        {
            return;
        }
        let Some(target) = self.target.clone() else {
            return;
        };
        self.bound = true;
        self.events.push_back(IoEvent::TargetBound {
            target,
            basis: self.last_basis.clone(),
        });
    }

    fn ask(&mut self, index: usize, req: ReqId, asked: &Asked, layer_basis: &IoBasis) {
        let io = &mut self.layers[index].layer.io;
        match asked {
            Asked::Asset(uuid) => io.resolve(req, *uuid, layer_basis),
            Asked::Path(path) => io.resolve_path(req, path, layer_basis),
        }
    }

    fn issue(&mut self, req: ReqId, asked: Asked, basis: &IoBasis) {
        let Some(bases) = self.layer_bases(basis) else {
            return self.foreign_basis(req, basis);
        };
        let slots = bases
            .iter()
            .map(|layer_basis| Slot {
                answer: Answer::Waiting,
                basis: layer_basis.clone(),
            })
            .collect::<Vec<_>>();
        for (index, slot) in slots.iter().enumerate() {
            self.ask(index, req, &asked, &slot.basis);
        }
        self.pending.insert(
            req,
            Pending::Resolve {
                asked,
                basis: basis.clone(),
                slots,
            },
        );
    }

    /// Ask layer `index` again, at its newest basis, every request whose
    /// answer from it is still owed (`owed`) or was unavailable.
    fn ask_again(&mut self, index: usize, owed: bool) {
        let reqs = self
            .pending
            .iter()
            .filter(|(_, pending)| match pending {
                Pending::Resolve { slots, .. } => match slots[index].answer {
                    Answer::Unavailable => true,
                    Answer::Waiting => owed,
                    Answer::Got(_) => false,
                },
                Pending::Fetch { .. } => false,
            })
            .map(|(req, _)| *req)
            .collect::<Vec<_>>();
        if reqs.is_empty() {
            return;
        }
        let layer_basis = self.layers[index].layer.io.begin_sweep();
        for req in reqs {
            let Some(Pending::Resolve {
                asked,
                basis,
                slots,
            }) = self.pending.remove(&req)
            else {
                continue;
            };
            let mut slots = slots;
            slots[index] = Slot {
                answer: Answer::Waiting,
                basis: layer_basis.clone(),
            };
            let entry = (basis.clone(), index, layer_basis.clone());
            if !self.reasked.contains(&entry) {
                self.reasked.push(entry);
            }
            self.ask(index, req, &asked, &layer_basis);
            self.pending.insert(
                req,
                Pending::Resolve {
                    asked,
                    basis,
                    slots,
                },
            );
        }
    }

    fn layer_event(&mut self, index: usize, event: IoEvent) {
        match event {
            IoEvent::Resolved { req, .. }
            | IoEvent::PathResolved { req, .. }
            | IoEvent::Fetched { req, .. }
            | IoEvent::RequestError { req, .. }
            | IoEvent::SnapshotExpired { req, .. } => self.answer(index, req, event),
            IoEvent::Delta {
                stamp,
                assets,
                paths,
            } => {
                // A layer's deletion uncovers the layers under it: the
                // asset re-resolves, and the stack answers whether it is
                // gone.
                let assets = if index == 0 {
                    assets
                } else {
                    assets
                        .into_iter()
                        .map(|(asset, _)| (asset, AssetDeltaState::Changed))
                        .collect()
                };
                self.events.push_back(IoEvent::Delta {
                    stamp,
                    assets,
                    paths,
                });
            }
            IoEvent::ReconnectRequired { reason } if self.layers[index].layer.optional => {
                self.lose_layer(index, format!("{reason:?}"));
            }
            IoEvent::TargetBound { target, basis } => {
                if self.layer_target(index).as_ref() != Some(&target) {
                    return;
                }
                self.layers[index].bound = true;
                self.ask_again(index, false);
                if self.bound {
                    self.publish_newly_named(&basis);
                }
            }
            IoEvent::TargetRejected { message } if self.layers[index].layer.optional => {
                self.events.push_back(IoEvent::ConnectionError {
                    message: format!("layer {index}: {message}"),
                });
            }
            event @ (IoEvent::ReconnectRequired { .. }
            | IoEvent::TargetRejected { .. }
            | IoEvent::ConnectionError { .. }
            | IoEvent::RestartRequired { .. }) => self.events.push_back(event),
        }
    }

    /// A layer bound after the stack: what it names is unknown, so every
    /// subscription re-resolves.
    fn publish_newly_named(&mut self, layer_basis: &IoBasis) {
        if self.assets.is_empty() && self.paths.is_empty() {
            return;
        }
        let stamp = layer_basis.rpc_snapshot().unwrap_or(SnapshotStamp {
            instance: StoreInstanceId([0; 16]),
            version: InputVersion(0),
        });
        self.events.push_back(IoEvent::Delta {
            stamp,
            assets: self
                .assets
                .iter()
                .map(|asset| (*asset, AssetDeltaState::Changed))
                .collect(),
            paths: self.paths.iter().cloned().collect(),
        });
    }

    /// An optional layer lost its connection: rebind it alone. Its
    /// requests were dropped with the connection; each resolve still owed
    /// is asked again of the unbound layer (what it named fails, the rest
    /// is unavailable), each fetch fails.
    fn lose_layer(&mut self, index: usize, reason: String) {
        let message = format!("layer {index} lost its connection ({reason}); reconnecting");
        self.layers[index].bound = false;
        self.events.push_back(IoEvent::ConnectionError {
            message: message.clone(),
        });
        if let Some(target) = self.layer_target(index) {
            self.layers[index].layer.io.bind_target(target);
        }
        let fetches = self
            .pending
            .iter()
            .filter(|(_, pending)| matches!(pending, Pending::Fetch { layer, .. } if *layer == index))
            .map(|(req, _)| *req)
            .collect::<Vec<_>>();
        for req in fetches {
            let event = IoEvent::RequestError {
                req,
                message: message.clone(),
                basis: IoBasis::Disconnected,
            };
            self.answer(index, req, event);
        }
        self.ask_again(index, true);
    }

    fn answer(&mut self, index: usize, req: ReqId, event: IoEvent) {
        let unavailable = self.unavailable(index);
        let Some(pending) = self.pending.get_mut(&req) else {
            return;
        };
        match pending {
            Pending::Fetch { basis, layer } => {
                if *layer != index {
                    return;
                }
                let basis = basis.clone();
                self.pending.remove(&req);
                self.events.push_back(rebased(event, basis));
            }
            Pending::Resolve { slots, .. } => {
                // The first answer stands.
                if matches!(event, IoEvent::Fetched { .. })
                    || !matches!(slots[index].answer, Answer::Waiting)
                {
                    return;
                }
                slots[index].answer = if unavailable && !names(&event) {
                    Answer::Unavailable
                } else {
                    Answer::Got(event)
                };
                self.settle(req);
            }
        }
    }

    /// Answer `req` once the topmost layer naming it is known.
    fn settle(&mut self, req: ReqId) {
        let Some(Pending::Resolve { slots, .. }) = self.pending.get(&req) else {
            return;
        };
        let mut chosen = None;
        for (index, slot) in slots.iter().enumerate().rev() {
            match &slot.answer {
                Answer::Waiting => return,
                Answer::Got(event) if names(event) => {
                    chosen = Some(index);
                    break;
                }
                Answer::Got(_) | Answer::Unavailable => {}
            }
        }
        // Nothing available names it: wait for the unavailable layers.
        if chosen.is_none()
            && slots
                .iter()
                .any(|slot| matches!(slot.answer, Answer::Unavailable))
        {
            return;
        }
        let Some(Pending::Resolve {
            asked,
            basis,
            mut slots,
        }) = self.pending.remove(&req)
        else {
            unreachable!("settled request is pending");
        };
        let event = match chosen {
            Some(index) => {
                let slot = std::mem::replace(&mut slots[index].answer, Answer::Waiting);
                let Answer::Got(event) = slot else {
                    unreachable!("chosen answer");
                };
                if let IoEvent::Resolved {
                    result: ResolveResult::Built { content_hash },
                    ..
                } = &event
                {
                    self.route(&basis, *content_hash, index, slots[index].basis.clone());
                }
                rebased(event, basis)
            }
            None => not_named(req, asked, basis, &slots),
        };
        self.events.push_back(event);
    }

    fn route(&mut self, basis: &IoBasis, content_hash: ContentHash, layer: usize, at: IoBasis) {
        match self.routes.iter_mut().find(|(live, _)| live == basis) {
            Some((_, routes)) => {
                routes.insert(content_hash, (layer, at));
            }
            None => self
                .routes
                .push((basis.clone(), HashMap::from([(content_hash, (layer, at))]))),
        }
    }

    /// The layer that answered `content_hash` under `basis`, else under any
    /// live basis (a payload carried across a rebase).
    fn route_of(&self, basis: &IoBasis, content_hash: ContentHash) -> Option<(usize, IoBasis)> {
        self.routes
            .iter()
            .find(|(live, _)| live == basis)
            .and_then(|(_, routes)| routes.get(&content_hash))
            .or_else(|| {
                self.routes
                    .iter()
                    .rev()
                    .find_map(|(_, routes)| routes.get(&content_hash))
            })
            .cloned()
    }

    /// Whether a live stack basis or a live re-ask still uses `layer_basis`
    /// at layer `index`.
    fn layer_basis_in_use(&self, index: usize, layer_basis: &IoBasis) -> bool {
        self.live
            .iter()
            .any(|live| live.layers().is_some_and(|live| live[index] == *layer_basis))
            || self
                .reasked
                .iter()
                .any(|(_, layer, basis)| *layer == index && basis == layer_basis)
    }
}

/// The answer when no layer names the request: the topmost deletion, else
/// missing; a path is missing when any layer has a path table to look in.
fn not_named(req: ReqId, asked: Asked, basis: IoBasis, slots: &[Slot]) -> IoEvent {
    let answers = || {
        slots.iter().rev().filter_map(|slot| match &slot.answer {
            Answer::Got(event) => Some(event),
            _ => None,
        })
    };
    match asked {
        Asked::Asset(uuid) => IoEvent::Resolved {
            req,
            uuid,
            result: answers()
                .find_map(|event| match event {
                    IoEvent::Resolved {
                        result: result @ ResolveResult::Deleted { .. },
                        ..
                    } => Some(result.clone()),
                    _ => None,
                })
                .unwrap_or(ResolveResult::Missing),
            basis,
        },
        Asked::Path(path) => IoEvent::PathResolved {
            req,
            path,
            result: if answers().any(|event| {
                matches!(
                    event,
                    IoEvent::PathResolved {
                        result: PathResolveResult::Missing,
                        ..
                    }
                )
            }) {
                PathResolveResult::Missing
            } else {
                PathResolveResult::Unsupported
            },
            basis,
        },
    }
}

/// Whether a layer's answer names its asset or path.
fn names(event: &IoEvent) -> bool {
    !matches!(
        event,
        IoEvent::Resolved {
            result: ResolveResult::Missing | ResolveResult::Deleted { .. },
            ..
        } | IoEvent::PathResolved {
            result: PathResolveResult::Missing | PathResolveResult::Unsupported,
            ..
        }
    )
}

/// A layer's request-terminal event under the stack basis it was issued
/// with.
fn rebased(event: IoEvent, stack: IoBasis) -> IoEvent {
    match event {
        IoEvent::Resolved {
            req, uuid, result, ..
        } => IoEvent::Resolved {
            req,
            uuid,
            result,
            basis: stack,
        },
        IoEvent::PathResolved {
            req, path, result, ..
        } => IoEvent::PathResolved {
            req,
            path,
            result,
            basis: stack,
        },
        IoEvent::Fetched {
            req,
            content_hash,
            artifact,
            ..
        } => IoEvent::Fetched {
            req,
            content_hash,
            artifact,
            basis: stack,
        },
        IoEvent::RequestError { req, message, .. } => IoEvent::RequestError {
            req,
            message,
            basis: stack,
        },
        IoEvent::SnapshotExpired { req, .. } => IoEvent::SnapshotExpired { req, basis: stack },
        other => other,
    }
}

impl LoaderIO for LayeredIO {
    fn bind_target(&mut self, target: RuntimeTarget) {
        self.target = Some(target);
        self.bound = false;
        self.pending.clear();
        self.routes.clear();
        self.reasked.clear();
        self.live.clear();
        for index in 0..self.layers.len() {
            self.layers[index].bound = false;
            let target = self.layer_target(index).expect("target set above");
            self.layers[index].layer.io.bind_target(target);
        }
        self.publish_bound();
    }

    fn begin_sweep(&mut self) -> IoBasis {
        let bases = self
            .layers
            .iter_mut()
            .map(|state| state.layer.io.begin_sweep())
            .collect::<Vec<_>>();
        let basis = IoBasis::Layers(Arc::from(bases));
        if !self.live.contains(&basis) {
            self.live.push(basis.clone());
        }
        self.last_basis = basis.clone();
        basis
    }

    fn end_sweep(&mut self, basis: &IoBasis) {
        let Some(bases) = self.layer_bases(basis) else {
            return;
        };
        self.live.retain(|live| live != basis);
        self.routes.retain(|(live, _)| live != basis);
        self.pending.retain(|_, pending| match pending {
            Pending::Resolve { basis: issued, .. } | Pending::Fetch { basis: issued, .. } => {
                issued != basis
            }
        });
        let mut ended = bases.iter().cloned().enumerate().collect::<Vec<_>>();
        let reasked = std::mem::take(&mut self.reasked);
        for (stack, index, layer_basis) in reasked {
            if stack == *basis {
                if !ended.contains(&(index, layer_basis.clone())) {
                    ended.push((index, layer_basis));
                }
            } else {
                self.reasked.push((stack, index, layer_basis));
            }
        }
        for (index, layer_basis) in ended {
            if !self.layer_basis_in_use(index, &layer_basis) {
                self.layers[index].layer.io.end_sweep(&layer_basis);
            }
        }
    }

    fn resolve(&mut self, req: ReqId, uuid: AssetUuid, basis: &IoBasis) {
        self.issue(req, Asked::Asset(uuid), basis);
    }

    fn fetch(&mut self, req: ReqId, content_hash: ContentHash, basis: &IoBasis) {
        if self.layer_bases(basis).is_none() {
            return self.foreign_basis(req, basis);
        }
        let Some((layer, at)) = self.route_of(basis, content_hash) else {
            self.events.push_back(IoEvent::RequestError {
                req,
                message: format!("no layer answered content hash {content_hash:?}"),
                basis: basis.clone(),
            });
            return;
        };
        self.pending.insert(
            req,
            Pending::Fetch {
                basis: basis.clone(),
                layer,
            },
        );
        self.layers[layer].layer.io.fetch(req, content_hash, &at);
    }

    fn resolve_path(&mut self, req: ReqId, path: &AssetPath, basis: &IoBasis) {
        self.issue(req, Asked::Path(path.clone()), basis);
    }

    fn subscribe(&mut self, uuid: AssetUuid) {
        self.assets.insert(uuid);
        for state in &mut self.layers {
            state.layer.io.subscribe(uuid);
        }
    }

    fn unsubscribe(&mut self, uuid: AssetUuid) {
        self.assets.remove(&uuid);
        for state in &mut self.layers {
            state.layer.io.unsubscribe(uuid);
        }
    }

    fn subscribe_path(&mut self, path: &str) {
        self.paths.insert(path.to_owned());
        for state in &mut self.layers {
            state.layer.io.subscribe_path(path);
        }
    }

    fn unsubscribe_path(&mut self, path: &str) {
        self.paths.remove(path);
        for state in &mut self.layers {
            state.layer.io.unsubscribe_path(path);
        }
    }

    fn poll(&mut self) -> Vec<IoEvent> {
        for index in 0..self.layers.len() {
            for event in self.layers[index].layer.io.poll() {
                self.layer_event(index, event);
            }
        }
        self.publish_bound();
        self.events.drain(..).collect()
    }
}
