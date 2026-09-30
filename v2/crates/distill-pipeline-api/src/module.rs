//! Module-authoring exports for the pipeline ABI.
//!
//! A pipeline cdylib supplies safe `register` and `unload` functions, derives
//! New Game Plus's shared `NgpSourceIdentity`, and invokes
//! [`export_pipeline_module_v2!`]. The generated unsafe surface is deliberately
//! small: one C ABI table entry, one C ABI identity probe, and two Rust ABI
//! calls that contain unwinding before it can cross the dynamic-library
//! boundary.

use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::epoch::ModuleCallError;
use crate::module_loader::{encode_module_abi_identity, host_module_abi_identity};

const PROBE_OK: i32 = 0;
const PROBE_INSUFFICIENT_CAPACITY: i32 = 1;
const PROBE_ERROR: i32 = -1;

/// The shared module-side implementation of the pre-Rust-ABI identity probe.
///
/// A null buffer with zero capacity is the required size-query form. On every
/// non-error return, `length` receives the complete required byte count. A
/// positive status means that the supplied buffer was too small.
///
/// # Safety
///
/// `length` must be null or point to writable `u32` storage. When `capacity`
/// is nonzero, `buffer` must be null or point to `capacity` writable bytes.
pub unsafe extern "C" fn module_abi_probe_v2(
    buffer: *mut u8,
    capacity: u32,
    length: *mut u32,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if length.is_null() {
            return Err(ModuleCallError::new(
                "pipeline ABI probe received a null length pointer",
            ));
        }
        let bytes = encode_module_abi_identity(&host_module_abi_identity())?;
        let required = u32::try_from(bytes.len())
            .map_err(|_| ModuleCallError::new("pipeline ABI identity exceeds u32"))?;

        // SAFETY: required by this function's caller contract and checked for
        // null immediately above.
        unsafe { length.write(required) };
        if capacity < required {
            return Ok(PROBE_INSUFFICIENT_CAPACITY);
        }
        if required != 0 && buffer.is_null() {
            return Err(ModuleCallError::new(
                "pipeline ABI probe received a null output buffer",
            ));
        }
        if required != 0 {
            // SAFETY: the caller promises `capacity` writable bytes and the
            // capacity check proves the complete payload fits.
            unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len()) };
        }
        Ok(PROBE_OK)
    }));

    match result {
        Ok(Ok(status)) => status,
        Ok(Err(_)) | Err(_) => PROBE_ERROR,
    }
}

#[doc(hidden)]
pub fn contain_module_call<T>(
    operation: &str,
    call: impl FnOnce() -> Result<T, ModuleCallError>,
) -> Result<T, ModuleCallError> {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|_| {
        Err(ModuleCallError::new(format!(
            "pipeline module {operation} panicked"
        )))
    })
}

/// Export a Distill v2 pipeline module table from safe author functions.
///
/// The register function must have the signature
/// `fn(&[TargetDefinition], &mut CandidateRegistrationArena)
/// -> Result<BTreeSet<String>, ModuleCallError>`. The unload function must be
/// `fn() -> Result<(), ModuleCallError>`. A module must also export the shared
/// source identity, normally with `#[derive(NgpSourceIdentity)]` and a
/// `build.rs` that calls `ngp_source_hash::build_script_main()`.
///
/// The macro may be invoked only once in a cdylib.
#[macro_export]
macro_rules! export_pipeline_module_v2 {
    (register = $register:path, unload = $unload:path $(,)?) => {
        unsafe fn __distill_pipeline_register_v2(
            targets: &[$crate::epoch::TargetDefinition],
            arena: &mut $crate::epoch::CandidateRegistrationArena,
        ) -> ::std::result::Result<
            ::std::collections::BTreeSet<::std::string::String>,
            $crate::epoch::ModuleCallError,
        > {
            $crate::module_sdk::contain_module_call("register", || $register(targets, arena))
        }

        unsafe fn __distill_pipeline_unload_v2(
        ) -> ::std::result::Result<(), $crate::epoch::ModuleCallError> {
            $crate::module_sdk::contain_module_call("unload", $unload)
        }

        static __DISTILL_PIPELINE_MODULE_TABLE_V2: $crate::module_loader::PipelineModuleTableV2 =
            $crate::module_loader::PipelineModuleTableV2 {
                abi_version: $crate::module_loader::PIPELINE_MODULE_ABI_VERSION_V2,
                module_abi: $crate::module_sdk::module_abi_probe_v2,
                register: __distill_pipeline_register_v2,
                unload: __distill_pipeline_unload_v2,
            };

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn distill_pipeline_module_v2(
        ) -> *const $crate::module_loader::PipelineModuleTableV2 {
            &__DISTILL_PIPELINE_MODULE_TABLE_V2
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module_loader::decode_module_abi_identity;

    #[test]
    fn probe_supports_size_query_and_exact_write() {
        let mut length = 0_u32;
        // SAFETY: null/zero is the documented size-query form and `length` is
        // valid writable storage.
        let status = unsafe { module_abi_probe_v2(std::ptr::null_mut(), 0, &mut length) };
        assert_eq!(status, PROBE_INSUFFICIENT_CAPACITY);
        assert_ne!(length, 0);

        let mut bytes = vec![0_u8; length as usize];
        // SAFETY: `bytes` supplies the exact reported capacity.
        let status = unsafe { module_abi_probe_v2(bytes.as_mut_ptr(), length, &mut length) };
        assert_eq!(status, PROBE_OK);
        assert_eq!(
            decode_module_abi_identity(&bytes).unwrap(),
            host_module_abi_identity()
        );
    }

    #[test]
    fn probe_and_module_calls_contain_panics() {
        // SAFETY: a null length pointer is accepted as a reported probe error.
        assert_eq!(
            unsafe { module_abi_probe_v2(std::ptr::null_mut(), 0, std::ptr::null_mut()) },
            PROBE_ERROR
        );
        let error = contain_module_call::<()>("register", || panic!("fixture panic")).unwrap_err();
        assert_eq!(error.detail(), "pipeline module register panicked");
    }
}
