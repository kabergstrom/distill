//! Concrete pipeline cdylib loader over the shared New Game Plus host layer.
//!
//! The module table, its ABI identity and the export macro are defined in
//! `distill-pipeline-api` and re-exported here.

use std::collections::BTreeSet;

pub use distill_pipeline_api::module::{
    decode_module_abi_identity, encode_module_abi_identity, host_interface_closure_manifest,
    host_module_abi_identity, host_rustc_identity, PipelineModuleEntryV2, PipelineModuleTableV2,
    PipelineProbeFnV2, PipelineRegisterFnV2, PipelineUnloadFnV2, PIPELINE_MODULE_ABI_VERSION_V2,
    PIPELINE_MODULE_SYMBOL_V2,
};

use crate::epoch::{
    CandidateRegistrationArena, LoadedPipelineModule, ModuleAbiIdentity, ModuleCallError,
    PipelineModuleLoader, StagedModule, TargetDefinition,
};

const MAX_PROBE_BYTES: usize = 64 * 1024;

/// Copy a versioned module table without reading beyond the cross-ABI prefix
/// before that prefix has selected the complete table layout.
///
/// # Safety
///
/// `pointer` must be null or point to readable, properly aligned storage for
/// at least one `u32`. If that prefix names v2, it must point to a complete
/// resident [`PipelineModuleTableV2`].
unsafe fn read_pipeline_module_table(
    pointer: *const PipelineModuleTableV2,
) -> Result<PipelineModuleTableV2, ModuleCallError> {
    if pointer.is_null() {
        return Err(ModuleCallError::new(
            "pipeline module returned a null function table",
        ));
    }
    // SAFETY: the C bootstrap contract guarantees the version prefix exists;
    // nothing after it is read until that prefix selects the v2 layout.
    let abi_version = unsafe { (pointer.cast::<u32>()).read() };
    if abi_version != PIPELINE_MODULE_ABI_VERSION_V2 {
        return Err(ModuleCallError::new(format!(
            "pipeline module ABI version {abi_version} is unsupported (expected {PIPELINE_MODULE_ABI_VERSION_V2})"
        )));
    }
    // SAFETY: a matching prefix selects the complete v2 table contract, and
    // the resident module keeps the pointed-to static alive.
    Ok(unsafe { pointer.read() })
}

#[derive(Debug, Default)]
pub struct DynamicPipelineModuleLoader;

impl PipelineModuleLoader for DynamicPipelineModuleLoader {
    fn open_staged(
        &mut self,
        staged: &StagedModule,
    ) -> Result<Box<dyn LoadedPipelineModule>, ModuleCallError> {
        // SAFETY: opening native code remains the explicitly audited module
        // boundary. The shared host verifies the staged bytes immediately
        // before opening exactly this host-owned path.
        let image = unsafe {
            ngp_module_host::HostedLibrary::open_verified(&staged.path, staged.content_hash)
        }
        .map_err(|error| ModuleCallError::new(error.to_string()))?;
        // SAFETY: `open_verified` authenticated and pinned the exact staged
        // image. The shared reader bounds and copies the four NGP data symbols
        // while that image remains resident.
        let source_identity = unsafe { ngp_module_host::read_source_identity(&image) }
            .map_err(|error| ModuleCallError::new(error.to_string()))?;
        let table = {
            // SAFETY: the symbol has a fixed C ABI and is not called through a
            // Rust ABI until the table's prefix version has been checked.
            let entry = unsafe { image.get::<PipelineModuleEntryV2>(PIPELINE_MODULE_SYMBOL_V2) }
                .map_err(|error| {
                    ModuleCallError::new(format!(
                        "pipeline module is missing distill_pipeline_module_v2: {error}"
                    ))
                })?;
            // SAFETY: the exported entry contract returns a resident static
            // table. It is copied while the image is pinned by `image`.
            let pointer = unsafe { entry() };
            // SAFETY: the fixed entry ABI guarantees at least the aligned C
            // version prefix. The helper reads the remaining v2 table only
            // after that prefix matches.
            unsafe { read_pipeline_module_table(pointer) }?
        };
        Ok(Box::new(DynamicLoadedPipelineModule {
            image: Some(image),
            table: Some(table),
            source_identity,
        }))
    }
}

struct DynamicLoadedPipelineModule {
    image: Option<ngp_module_host::HostedLibrary>,
    table: Option<PipelineModuleTableV2>,
    source_identity: ngp_module_host::ModuleSourceIdentity,
}

impl DynamicLoadedPipelineModule {
    fn table(&self) -> Result<PipelineModuleTableV2, ModuleCallError> {
        self.table
            .ok_or_else(|| ModuleCallError::new("pipeline module is already closed"))
    }
}

impl LoadedPipelineModule for DynamicLoadedPipelineModule {
    fn source_identity(
        &mut self,
    ) -> Result<ngp_module_host::ModuleSourceIdentity, ModuleCallError> {
        self.table()?;
        Ok(self.source_identity.clone())
    }

    fn module_abi(&mut self) -> Result<ModuleAbiIdentity, ModuleCallError> {
        decode_module_abi_identity(&read_probe(self.table()?.module_abi)?)
    }

    fn register(
        &mut self,
        targets: &[TargetDefinition],
        arena: &mut CandidateRegistrationArena,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        // SAFETY: this Rust-ABI entry is reached only after the host compared
        // the shared source identity and ModuleAbiIdentity exactly. The module
        // registers through the API arena, which calls back into `arena`
        // through its `RegistrationHost` implementation.
        unsafe { (self.table()?.register)(targets, &mut arena.registrar()) }
    }

    fn unload(&mut self) -> Result<(), ModuleCallError> {
        // SAFETY: same checked Rust-ABI contract as `register`.
        unsafe { (self.table()?.unload)() }
    }

    fn dlclose(&mut self) {
        // Invalidate every copied call target before releasing the image. Even
        // accidental trait-object reuse can now return only a closed-state
        // error rather than jumping through a resident-table pointer.
        self.table = None;
        if let Some(image) = self.image.take() {
            image.close();
        }
    }
}

fn read_probe(probe: PipelineProbeFnV2) -> Result<Vec<u8>, ModuleCallError> {
    let mut required = 0_u32;
    // SAFETY: the C prefix explicitly permits a null buffer with zero
    // capacity and writes only the required length through `required`.
    let status = unsafe { probe(std::ptr::null_mut(), 0, &mut required) };
    if status < 0 {
        return Err(ModuleCallError::new(format!(
            "pipeline probe rejected its size query with status {status}"
        )));
    }
    let required = usize::try_from(required)
        .map_err(|_| ModuleCallError::new("pipeline probe size does not fit usize"))?;
    if required > MAX_PROBE_BYTES {
        return Err(ModuleCallError::new(format!(
            "pipeline probe requested {required} bytes (limit {MAX_PROBE_BYTES})"
        )));
    }
    let mut bytes = vec![0_u8; required];
    let mut written = u32::try_from(required)
        .map_err(|_| ModuleCallError::new("pipeline probe size does not fit u32"))?;
    // SAFETY: `bytes` provides exactly `written` writable bytes. The C prefix
    // contract reports failure as status and never unwinds.
    let status = unsafe { probe(bytes.as_mut_ptr(), written, &mut written) };
    if status < 0 {
        return Err(ModuleCallError::new(format!(
            "pipeline probe rejected its write with status {status}"
        )));
    }
    let written = usize::try_from(written)
        .map_err(|_| ModuleCallError::new("pipeline probe result does not fit usize"))?;
    if written > bytes.len() {
        return Err(ModuleCallError::new(
            "pipeline probe grew between size and write calls",
        ));
    }
    bytes.truncate(written);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn rejected_probe(
        _buffer: *mut u8,
        _capacity: u32,
        _length: *mut u32,
    ) -> i32 {
        -1
    }

    unsafe fn empty_register(
        _targets: &[TargetDefinition],
        _arena: &mut distill_pipeline_api::registration::RegistrationArena<'_>,
    ) -> Result<BTreeSet<String>, ModuleCallError> {
        Ok(BTreeSet::new())
    }

    unsafe fn empty_unload() -> Result<(), ModuleCallError> {
        Ok(())
    }

    #[test]
    fn closed_module_invalidates_every_copied_call_target() {
        let mut module = DynamicLoadedPipelineModule {
            image: None,
            table: Some(PipelineModuleTableV2 {
                abi_version: PIPELINE_MODULE_ABI_VERSION_V2,
                module_abi: rejected_probe,
                register: empty_register,
                unload: empty_unload,
            }),
            source_identity: ngp_module_host::ModuleSourceIdentity {
                crate_name: "pipeline".to_owned(),
                source_hash: "0123456789abcdef".to_owned(),
            },
        };

        module.dlclose();
        assert!(module
            .module_abi()
            .unwrap_err()
            .detail()
            .contains("already closed"));
        assert!(module
            .unload()
            .unwrap_err()
            .detail()
            .contains("already closed"));
    }

    #[test]
    fn incompatible_prefix_is_rejected_before_the_complete_table_is_read() {
        let prefix_only = PIPELINE_MODULE_ABI_VERSION_V2 + 1;
        let pointer = std::ptr::from_ref(&prefix_only).cast::<PipelineModuleTableV2>();

        // SAFETY: the test provides exactly the prefix storage promised for
        // an incompatible table. Reading a complete table would be invalid.
        let error = match unsafe { read_pipeline_module_table(pointer) } {
            Ok(_) => panic!("incompatible prefix unexpectedly selected a complete table"),
            Err(error) => error,
        };

        assert!(error.detail().contains("unsupported"));
    }
}
