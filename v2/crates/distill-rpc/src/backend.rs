use std::sync::Arc;

use crate::*;

/// Pure preparation result returned by an injected authoring backend. The RPC
/// server validates and commits this change set atomically against the caller's
/// input-version precondition.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedImportCommit {
    pub bundle: BundleUuid,
    pub commit: Commit,
}

pub trait DeferredOperation: Send + Sync {
    /// Execute the already-validated operation against `base`. The RPC server
    /// invokes this only when the client consumes the terminal Completed
    /// event, inside the input open on `store` that publishes it.
    fn complete(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
    ) -> Result<DeferredOperationResult, String>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeferredOperationResult {
    /// What the operation publishes in its own input; `None` for one whose
    /// effect is files the watcher publishes (a rename).
    pub commit: Option<Commit>,
    /// A per-file operation may change earlier files before a later
    /// conflict. This terminal diagnostic changes Completed to Failed.
    pub terminal_error: Option<String>,
}

/// A read-only operation: a report on one read snapshot. It holds no write
/// lock, writes nothing and publishes no version.
pub trait ReportOperation: Send + Sync {
    /// Report on `snapshot`, a read snapshot of the operation's base.
    /// `Ok(None)` completes the operation; `Ok(Some(findings))` fails it
    /// with them.
    fn report(
        &self,
        snapshot: &crate::server::ReportSnapshot<'_>,
    ) -> Result<Option<String>, String>;
}

#[derive(Clone)]
pub enum PreparedOperationPublication {
    Immediate(Box<Commit>),
    Deferred(Arc<dyn DeferredOperation>),
    Report(Arc<dyn ReportOperation>),
}

impl std::fmt::Debug for PreparedOperationPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Immediate(commit) => formatter.debug_tuple("Immediate").field(commit).finish(),
            Self::Deferred(_) => formatter.write_str("Deferred(..)"),
            Self::Report(_) => formatter.write_str("Report(..)"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedOperationCommit {
    pub publication: PreparedOperationPublication,
    pub progress: Vec<AuthoringProgressEvent>,
}

impl PreparedOperationCommit {
    pub fn immediate(commit: Commit, progress: Vec<AuthoringProgressEvent>) -> Self {
        Self {
            publication: PreparedOperationPublication::Immediate(Box::new(commit)),
            progress,
        }
    }

    pub fn deferred(
        operation: Arc<dyn DeferredOperation>,
        progress: Vec<AuthoringProgressEvent>,
    ) -> Self {
        Self {
            publication: PreparedOperationPublication::Deferred(operation),
            progress,
        }
    }

    pub fn report(
        operation: Arc<dyn ReportOperation>,
        progress: Vec<AuthoringProgressEvent>,
    ) -> Self {
        Self {
            publication: PreparedOperationPublication::Report(operation),
            progress,
        }
    }
}

/// The publishing step of an import run: publish what ran on a worker, or
/// fail. It runs inside the input open on the writer it is given, which
/// then publishes it. `Ok(Err(failure))` is a failure the step memoized:
/// its writes commit, with no new version. On `Err` nothing it wrote
/// commits.
pub type ImportJob = Box<
    dyn FnOnce(
            &mut distill_store::Store,
        ) -> Result<Result<PreparedImportCommit, RpcFailure>, RpcFailure>
        + Send,
>;

/// Daemon integration seam for workflows that require importer, filesystem,
/// migration, or doctor services. Implementations prepare a side-effect-free
/// commit; publication remains an atomic RPC-server CAS step.
pub trait AuthoringBackend: Send + Sync + 'static {
    /// The bytes of file `path` under root `root` as they are on disk now,
    /// read through the daemon's root identity checks. An `Err` is a file
    /// that cannot be read now, which a reader verifying a published hash
    /// treats as drift.
    fn read_file(
        &self,
        snapshot: &distill_store::StoreReader,
        root: &str,
        path: &str,
    ) -> Result<Vec<u8>, String>;

    /// The pipeline failure of the compiled state `snapshot` serves: its
    /// candidate's, or the runtime failure its epoch latched since it was
    /// published. It lives in this process, never in the store. An error:
    /// the backend cannot see the compiled state `snapshot` serves (a retryable failure, such as
    /// [`RpcFailure::SnapshotExpired`]), so it claims nothing about it.
    fn pipeline_failure(
        &self,
        _snapshot: &distill_store::StoreReader,
    ) -> Result<Option<PipelineFailure>, RpcFailure> {
        Ok(None)
    }

    /// Apply an ordinary authoring batch, planned against `base`, to the
    /// files it changes, and return what it wrote. The write is complete
    /// once the files are atomically on disk: the store follows through the
    /// watcher, as for any other edit. An error means no file changed.
    ///
    /// The RPC server invokes this inside an input open on `store` at
    /// `base`, which it then rolls back: the input holds the write lock, so
    /// no other writer publishes between the plan and the files. It must
    /// not call back into the [`crate::Server`].
    fn write_files(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        operations: &[AuthoringOp],
        force_lossy: bool,
    ) -> Result<WriteReceipt, RpcFailure>;

    fn prepare_import(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        request: &ImportRequest,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    fn prepare_reimport(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<PreparedImportCommit, RpcFailure>;

    /// Run an import on a worker, off the RPC thread (on a reader or writer
    /// of the backend's own), and return the step that publishes it while
    /// still at `base`. The default leaves all the work to that step.
    fn run_import(
        self: Arc<Self>,
        base: InputVersion,
        request: ImportRequest,
    ) -> Result<ImportJob, RpcFailure> {
        Ok(Box::new(move |store| {
            self.prepare_import(store, base, &request).map(Ok)
        }))
    }

    /// [`AuthoringBackend::run_import`] for a reimport.
    fn run_reimport(
        self: Arc<Self>,
        base: InputVersion,
        bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        Ok(Box::new(move |store| {
            self.prepare_reimport(store, base, bundle).map(Ok)
        }))
    }

    /// Check an operation against `store` and describe its progress; a
    /// deferred publication runs later, in its own input.
    fn prepare_operation(
        &self,
        store: &mut distill_store::Store,
        base: InputVersion,
        operation: &LongRunningOp,
    ) -> Result<PreparedOperationCommit, RpcFailure>;

    /// The path of `type_uuid` in the schema `snapshot`'s compiled state
    /// serves (`game::Spinner`); `None` for a type it does not know.
    fn type_name(&self, _snapshot: &distill_store::StoreReader, _type_uuid: TypeUuid) -> Option<String> {
        None
    }

    /// The files under root `root` (bundle files excepted) that `glob`
    /// matches (all without one), path-sorted, each with the import layer
    /// that claims it at `snapshot` (§8 "Default imports" precedence).
    fn source_files(
        &self,
        _snapshot: &distill_store::StoreReader,
        _root: &str,
        _glob: Option<&str>,
    ) -> Result<Vec<SourceFile>, RpcFailure> {
        Err(RpcFailure::InvalidAuthoringRequest {
            detail: "this daemon lists no source files".to_owned(),
        })
    }

    /// Run a release of explicit import `bundle` (`Hub.release`) on a
    /// worker, as [`AuthoringBackend::run_reimport`] runs a reimport.
    fn run_release(
        self: Arc<Self>,
        _base: InputVersion,
        _bundle: BundleUuid,
    ) -> Result<ImportJob, RpcFailure> {
        Err(RpcFailure::InvalidAuthoringRequest {
            detail: "this daemon releases no imports".to_owned(),
        })
    }

    /// What can be created at `snapshot` (§17 "Creating"): the authored
    /// types and importers of its compiled state's pipeline.
    fn authorable(&self, _snapshot: &distill_store::StoreReader) -> Result<Authorable, RpcFailure> {
        Err(RpcFailure::InvalidAuthoringRequest {
            detail: "this daemon declares nothing authorable".to_owned(),
        })
    }
}
/// What a requester reads while it starts or answers a build: its own
/// snapshot, whose answers decide whether a result serves it, and the
/// store's latest committed state, where shared build results and artifacts
/// land. Both are readers: nothing that answers a resolve writes, and a
/// thread holding an open input has no view to offer.
#[derive(Clone, Copy)]
pub struct BuildView<'a> {
    pub snapshot: &'a distill_store::StoreReader,
    pub stamp: SnapshotStamp,
    pub latest: &'a distill_store::StoreReader,
}

/// A build's answer for one requester, at that requester's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildAnswer {
    /// The requested artifact is in the CAS.
    Built {
        content_hash: ContentHash,
    },
    Failed {
        error: String,
    },
    /// An input the build observed differs at the requester's snapshot; a
    /// fresher snapshot resolves it.
    Drifted {
        input: DriftedInput,
    },
}

/// A finished build, still to be answered at each waiter's own snapshot.
pub trait BuildCompletion: Send {
    fn answer(self: Box<Self>, view: BuildView<'_>) -> Result<BuildAnswer, RpcFailure>;
}

/// One requester's interest in a submitted build. It resolves once the
/// build finishes; dropping it first withdraws the interest, and a build no
/// requester wants any more is not started.
pub struct BuildTicket(
    std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn BuildCompletion>> + Send>>,
);

impl BuildTicket {
    pub fn new(
        future: impl std::future::Future<Output = Box<dyn BuildCompletion>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl std::future::Future for BuildTicket {
    type Output = Box<dyn BuildCompletion>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl std::fmt::Debug for BuildTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildTicket").finish_non_exhaustive()
    }
}

/// How a backend takes up a request.
#[derive(Debug)]
pub enum BuildStart {
    /// Answered at once, at the requester's snapshot (a cache hit, or a
    /// failure found before any build).
    Answered(Result<BuildAnswer, RpcFailure>),
    /// Submitted: the requester awaits the ticket, then asks the completion
    /// for its own answer.
    Submitted(BuildTicket),
}

/// Daemon integration seam for target-specific lazy builds. The backend
/// owns the shared build state: it keys a build by its static inputs,
/// shares one build among every requester of that key, publishes the
/// result to the CAS and notifies the requesters. The RPC server only reads.
/// Implementations must not call back into the [`crate::Server`].
pub trait BuildBackend: Send + Sync {
    /// Answer `request` at `view`, or submit its build and hand back a
    /// ticket. Called on a connection's thread: it reads, never blocks on a
    /// build.
    fn start(&self, view: BuildView<'_>, request: &BuildRequest) -> BuildStart;

    /// The runtime policy of `request`'s type under the compiled state
    /// `snapshot` sees.
    fn runtime_type_policy(
        &self,
        _snapshot: &distill_store::StoreReader,
        _request: &RuntimeTypePolicyRequest,
    ) -> Result<RuntimeTypePolicy, RpcFailure> {
        Err(RpcFailure::AuthoringBackendUnavailable {
            operation: "runtime type-policy lookup".to_owned(),
        })
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkStream {
    pub(crate) structural: Arc<[u8]>,
    pub(crate) blobs: Vec<Arc<[u8]>>,
    pub(crate) chunk_size: usize,
    pub(crate) section: usize,
    pub(crate) offset: usize,
    pub(crate) total_bytes: u64,
    pub(crate) load_edges: Vec<ServedLoadEdge>,
}

impl ChunkStream {
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn load_edges(&self) -> &[ServedLoadEdge] {
        &self.load_edges
    }
}

pub(crate) trait ProgressCompletion {
    /// Check and publish the completion, on the connection's own writer.
    fn complete(&self) -> Result<(), String>;
    fn cancel(&self) -> bool;
}

pub struct ProgressStream {
    pub(crate) events: std::collections::VecDeque<AuthoringProgressEvent>,
    pub(crate) next_sequence: u64,
    pub(crate) terminal_seen: bool,
    pub(crate) completion: std::rc::Rc<dyn ProgressCompletion>,
}

impl std::fmt::Debug for ProgressStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProgressStream")
            .field("events", &self.events)
            .field("next_sequence", &self.next_sequence)
            .field("terminal_seen", &self.terminal_seen)
            .finish_non_exhaustive()
    }
}

impl ProgressStream {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn cancel(&mut self) -> bool {
        if self.terminal_seen || !self.completion.cancel() {
            return false;
        }
        self.events.clear();
        self.events.push_back(AuthoringProgressEvent {
            sequence: self.next_sequence,
            state: AuthoringProgressState::Cancelled,
            payload: Arc::from([]),
        });
        true
    }
}

impl ProgressStream {
    /// The next event. A completion publishes before it is answered: a
    /// failed publication turns it into `Failed`.
    fn next_event(&mut self) -> Option<AuthoringProgressEvent> {
        let event = self.events.pop_front()?;
        if event.state == AuthoringProgressState::Completed {
            let outcome = self.completion.complete();
            return Some(self.finish_completion(event, outcome));
        }
        if matches!(
            event.state,
            AuthoringProgressState::Cancelled | AuthoringProgressState::Failed
        ) {
            self.completion.cancel();
        }
        Some(self.settle(event))
    }

    /// The completion event, once its publication ran.
    fn finish_completion(
        &mut self,
        mut event: AuthoringProgressEvent,
        outcome: Result<(), String>,
    ) -> AuthoringProgressEvent {
        if let Err(error) = outcome {
            event.state = AuthoringProgressState::Failed;
            event.payload = Arc::from(error.into_bytes());
        }
        self.settle(event)
    }

    fn settle(&mut self, event: AuthoringProgressEvent) -> AuthoringProgressEvent {
        self.next_sequence = event.sequence.saturating_add(1);
        self.terminal_seen = event.state.is_terminal();
        event
    }
}

impl Iterator for ProgressStream {
    type Item = AuthoringProgressEvent;

    /// Runs a completion's publication on this thread.
    fn next(&mut self) -> Option<Self::Item> {
        self.next_event()
    }
}

impl ChunkStream {
    pub fn next_chunk(&mut self) -> Option<ArtifactChunk> {
        if self.section > self.blobs.len() {
            return None;
        }
        let (kind, bytes) = if self.section == 0 {
            (ArtifactChunkKind::Structural, self.structural.as_ref())
        } else {
            (
                ArtifactChunkKind::Blob {
                    index: (self.section - 1) as u32,
                },
                self.blobs[self.section - 1].as_ref(),
            )
        };
        if bytes.is_empty() {
            self.section += 1;
            self.offset = 0;
            return Some(ArtifactChunk {
                kind,
                offset: 0,
                bytes: Arc::from([]),
            });
        }
        let start = self.offset;
        let end = start.saturating_add(self.chunk_size).min(bytes.len());
        let chunk = Arc::from(&bytes[start..end]);
        self.offset = end;
        if end == bytes.len() {
            self.section += 1;
            self.offset = 0;
        }
        Some(ArtifactChunk {
            kind,
            offset: start as u64,
            bytes: chunk,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.section > self.blobs.len()
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectOutcome {
    Connected(Connected),
    ConfigurationFailed(ConfigurationError),
    PipelineUnavailable(PipelineUnavailableDiagnostic),
    Rejected(ConnectError),
    /// Past `max_connections`: the request was valid; retry later.
    Refused(RpcFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataConnectOutcome {
    Connected(MetadataConnected),
    ProtocolMismatch {
        expected: u32,
        observed: u32,
    },
    /// Past `max_connections`; retry later.
    Refused(RpcFailure),
}

impl MetadataConnectOutcome {
    pub fn connected(self) -> Option<MetadataConnected> {
        match self {
            Self::Connected(connected) => Some(connected),
            Self::ProtocolMismatch { .. } | Self::Refused(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataConnected {
    pub hub: crate::MetadataHub,
    pub instance: StoreInstanceId,
    pub protocol_epoch: u32,
}

impl PartialEq for MetadataConnected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.protocol_epoch == other.protocol_epoch
            && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for MetadataConnected {}

#[derive(Debug, Clone)]
pub struct Connected {
    pub hub: crate::Hub,
    pub instance: StoreInstanceId,
}

impl PartialEq for Connected {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance && self.hub.connection_id() == other.hub.connection_id()
    }
}

impl Eq for Connected {}

/// What [`AuthoringSnapshot::blob`](crate::AuthoringSnapshot::blob)
/// answers: an entry's blob, by its index in the entry's blob table.
#[derive(Debug)]
pub enum AuthoringBlobResult {
    /// The bytes as one chunk stream section (`Structural`), read at
    /// `stamp`.
    Blob { stamp: SnapshotStamp, chunks: ChunkStream },
    Missing,
    /// The entry's bundle file no longer holds the published bytes.
    Drifted { input: DriftedInput, current: SnapshotStamp },
}
#[derive(Debug, Clone)]
pub struct SubscriptionInstall {
    pub deltas: crate::DeltaStream,
    pub installed: InputVersion,
}

