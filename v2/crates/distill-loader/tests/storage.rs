use std::collections::BTreeSet;

use distill_asset::{ErasedValue, ModuleEpochToken};
use distill_core::id::TypeUuid;
use distill_loader::{
    AdoptionId, AssetStorage, GameModuleEpoch, HandleId, PendingState, PendingToken,
    RuntimeEpochError, RuntimeEpochs, StorageError, StoredAdoption, UpdateResult,
};
use distill_wire::native::CallbackPanic;

#[derive(Default)]
struct Storage {
    fail: BTreeSet<HandleId>,
    freed: Vec<HandleId>,
}

impl AssetStorage for Storage {
    fn update(
        &mut self,
        _type_uuid: TypeUuid,
        _handle: HandleId,
        _value: ErasedValue,
        _adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError> {
        unreachable!()
    }

    fn poll(&mut self, _token: PendingToken) -> PendingState {
        unreachable!()
    }
    fn commit(&mut self, _type_uuid: TypeUuid, _handle: HandleId, _adoption: AdoptionId) {}

    fn free(
        &mut self,
        _type_uuid: TypeUuid,
        handle: HandleId,
        _adoption: AdoptionId,
    ) -> Result<(), CallbackPanic> {
        self.freed.push(handle);
        if self.fail.contains(&handle) {
            Err(CallbackPanic)
        } else {
            Ok(())
        }
    }
}

fn stored(handle: u64) -> StoredAdoption {
    StoredAdoption {
        type_uuid: TypeUuid([1; 16]),
        handle: HandleId(handle),
        adoption: AdoptionId(handle),
    }
}

#[test]
fn successful_drain_frees_values_before_forgetting_module_resources() {
    let token = ModuleEpochToken::new(7);
    let epoch = GameModuleEpoch(7);
    let mut epochs = RuntimeEpochs::default();
    epochs.register(epoch, token, 2).unwrap();
    epochs.record_placeholder(epoch).unwrap();
    epochs.record_plan(epoch).unwrap();
    epochs.record_fetch(epoch).unwrap();
    epochs.record_adoption(epoch, stored(1)).unwrap();
    epochs.record_adoption(epoch, stored(2)).unwrap();
    epochs.begin_module_drain(epoch).unwrap();
    assert!(!epochs.can_issue_work(epoch));
    let mut storage = Storage::default();
    epochs.drain(epoch, &mut storage).unwrap();
    assert_eq!(storage.freed, vec![HandleId(1), HandleId(2)]);
    assert!(epochs.drain_complete(epoch));
}

#[test]
fn one_failed_free_poisons_exact_epoch_but_other_frees_continue() {
    let token = ModuleEpochToken::new(8);
    let epoch = GameModuleEpoch(8);
    let mut epochs = RuntimeEpochs::default();
    epochs.register(epoch, token.clone(), 1).unwrap();
    epochs.record_adoption(epoch, stored(1)).unwrap();
    epochs.record_adoption(epoch, stored(2)).unwrap();
    epochs.begin_module_drain(epoch).unwrap();
    let mut storage = Storage {
        fail: BTreeSet::from([HandleId(1)]),
        freed: vec![],
    };
    assert_eq!(
        epochs.drain(epoch, &mut storage),
        Err(RuntimeEpochError::FreeFailed {
            epoch,
            adoption: stored(1)
        })
    );
    assert_eq!(storage.freed, vec![HandleId(1), HandleId(2)]);
    assert!(token.is_poisoned());
    assert!(epochs.is_poisoned(epoch));
    assert!(!epochs.drain_complete(epoch));
}

#[test]
fn published_runtime_poison_fences_new_work_even_before_drain() {
    let token = ModuleEpochToken::new(9);
    let epoch = GameModuleEpoch(9);
    let mut epochs = RuntimeEpochs::default();
    epochs.register(epoch, token.clone(), 0).unwrap();
    assert!(epochs.can_issue_work(epoch));
    token.poison();
    assert!(!epochs.can_issue_work(epoch));
    assert_eq!(
        epochs.record_plan(epoch),
        Err(RuntimeEpochError::Fenced(epoch))
    );
}
