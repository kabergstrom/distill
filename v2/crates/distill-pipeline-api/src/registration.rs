//! Registering callbacks with the host.
//!
//! A module's `register` receives a [`RegistrationArena`]. Its generic
//! `register_*` methods run in the module: they erase the callback into a
//! boxed payload whose cleanup and call thunks are monomorphized there, and
//! hand it to the host through the one non-generic [`RegistrationHost`] call.

use crate::callbacks::{
    cleanup_callback, erase_callback, CallbackHandle, CodegenDescriptor, DefaultsDescriptor,
    ImporterDescriptor, MigrationKey, PipelineCodegen, PipelineDefaults, PipelineImporter,
    PipelineMigration, PipelineProcessor, PipelineValidator, ProcessorDescriptor, ToolDescriptor,
    ValidatorDescriptor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDefinition {
    pub name: String,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RegistrationKind {
    Importer,
    Processor,
    Codegen,
    Validator,
    Migration,
    Defaults,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub kind: RegistrationKind,
    pub id: String,
    pub version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationDisposition {
    Consumed,
}

/// Every object-bearing host callback returns an explicit consumed status on
/// both success and failure. There is deliberately no returned-to-caller arm.
#[derive(Debug)]
#[must_use = "registration ownership was consumed; inspect the nested result"]
pub struct RegistrationStatus {
    pub disposition: RegistrationDisposition,
    pub result: Result<(), ModuleCallError>,
}

impl RegistrationStatus {
    pub fn into_result(self) -> Result<(), ModuleCallError> {
        debug_assert_eq!(self.disposition, RegistrationDisposition::Consumed);
        self.result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleCallError {
    detail: String,
}

impl ModuleCallError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for ModuleCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for ModuleCallError {}

/// A module-owned callback on its way to the host: the boxed object, the
/// thunk that destroys it, and the handle whose thunks call it.
///
/// This has deliberately no `Drop` implementation. Dropping it without
/// [`Self::into_parts`] leaks the object rather than running module drop
/// glue outside its contained cleanup thunk.
pub struct ErasedCallback {
    pointer: *mut u8,
    cleanup: CleanupFn,
    handle: CallbackHandle,
}

/// Destroys an erased callback under containment. On `Ok` the object is
/// gone; on `Err` it is still allocated and must be leaked.
pub type CleanupFn = unsafe fn(*mut u8) -> Result<(), ModuleCallError>;

impl std::fmt::Debug for ErasedCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedCallback")
            .field("pointer", &self.pointer)
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl ErasedCallback {
    fn new<T: Send + Sync + 'static>(callback: T, handle: CallbackHandle) -> Self {
        Self {
            pointer: erase_callback(callback),
            cleanup: cleanup_callback::<T>,
            handle,
        }
    }

    /// Take the payload apart. The caller now owns `pointer`: it stays valid
    /// until `cleanup` returns `Ok`, which may be called at most once, and
    /// `handle`'s thunks may be called with it until then.
    pub fn into_parts(self) -> (*mut u8, CleanupFn, CallbackHandle) {
        (self.pointer, self.cleanup, self.handle)
    }
}

/// The host side of a [`RegistrationArena`].
pub trait RegistrationHost {
    /// Take one callback. It is consumed on entry, whether the status is
    /// success or rejection; the host destroys a rejected callback with its
    /// cleanup thunk. Implementations contain their own panics.
    fn install_callback(
        &mut self,
        registration: Registration,
        callback: ErasedCallback,
    ) -> RegistrationStatus;
}

/// What a module's `register` function receives.
pub struct RegistrationArena<'a> {
    host: &'a mut dyn RegistrationHost,
}

impl<'a> RegistrationArena<'a> {
    #[doc(hidden)]
    pub fn new(host: &'a mut dyn RegistrationHost) -> Self {
        Self { host }
    }

    pub fn register_importer<T: PipelineImporter>(
        &mut self,
        descriptor: ImporterDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Importer,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::importer::<T>(descriptor));
        self.host.install_callback(registration, callback)
    }

    pub fn register_processor<T: PipelineProcessor>(
        &mut self,
        descriptor: ProcessorDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Processor,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::processor::<T>(descriptor));
        self.host.install_callback(registration, callback)
    }

    pub fn register_codegen<T: PipelineCodegen>(
        &mut self,
        descriptor: CodegenDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Codegen,
            id: descriptor.id.clone(),
            version: descriptor.version,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::codegen::<T>(descriptor));
        self.host.install_callback(registration, callback)
    }

    pub fn register_validator<T: PipelineValidator>(
        &mut self,
        descriptor: ValidatorDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Validator,
            id: descriptor.id.clone(),
            version: 1,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::validator::<T>(descriptor));
        self.host.install_callback(registration, callback)
    }

    pub fn register_migration<T: PipelineMigration>(
        &mut self,
        key: MigrationKey,
        callback: T,
    ) -> RegistrationStatus {
        let key = key.id();
        let registration = Registration {
            kind: RegistrationKind::Migration,
            id: key.clone(),
            version: 1,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::migration::<T>(key));
        self.host.install_callback(registration, callback)
    }

    pub fn register_defaults<T: PipelineDefaults>(
        &mut self,
        descriptor: DefaultsDescriptor,
        callback: T,
    ) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Defaults,
            id: descriptor.type_uuid.to_string(),
            version: 1,
        };
        let callback = ErasedCallback::new(callback, CallbackHandle::defaults::<T>(descriptor));
        self.host.install_callback(registration, callback)
    }

    /// Register `T`'s generated `DefaultTable` as its migration defaults, so
    /// an automatic migration adding a field fills it from `Default`.
    pub fn register_asset_defaults<T: distill_asset::AssetDefaults>(
        &mut self,
    ) -> RegistrationStatus {
        self.register_defaults(
            DefaultsDescriptor {
                type_uuid: T::TYPE_UUID,
            },
            crate::asset_defaults::AssetTableDefaults::<T>::new(),
        )
    }

    pub fn register_tool(&mut self, descriptor: ToolDescriptor) -> RegistrationStatus {
        let registration = Registration {
            kind: RegistrationKind::Tool,
            id: descriptor.id.clone(),
            version: 1,
        };
        let callback = ErasedCallback::new(descriptor.clone(), CallbackHandle::Tool(descriptor));
        self.host.install_callback(registration, callback)
    }
}
