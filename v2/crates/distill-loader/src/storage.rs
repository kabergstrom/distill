//! Status-returning storage and module-epoch drain accounting.

use std::collections::{BTreeMap, BTreeSet};

use distill_asset::{ErasedValue, ModuleEpochPoisonCause, ModuleEpochToken};
use distill_core::id::TypeUuid;
use distill_wire::native::CallbackPanic;

use crate::runtime::{AdoptionId, HandleId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GameModuleEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PendingToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    Engine(String),
    Callback {
        panic: CallbackPanic,
        owner_epoch: GameModuleEpoch,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResult {
    Ready,
    Pending(PendingToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingState {
    Ready,
    Pending,
    Failed(StorageError),
}

pub trait AssetStorage {
    fn update(
        &mut self,
        type_uuid: TypeUuid,
        handle: HandleId,
        value: ErasedValue,
        adoption: AdoptionId,
    ) -> Result<UpdateResult, StorageError>;

    fn poll(&mut self, token: PendingToken) -> PendingState;

    fn commit(&mut self, type_uuid: TypeUuid, handle: HandleId, adoption: AdoptionId);

    fn free(
        &mut self,
        type_uuid: TypeUuid,
        handle: HandleId,
        adoption: AdoptionId,
    ) -> Result<(), CallbackPanic>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoredAdoption {
    pub type_uuid: TypeUuid,
    pub handle: HandleId,
    pub adoption: AdoptionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeEpochError {
    DuplicateEpoch(GameModuleEpoch),
    UnknownEpoch(GameModuleEpoch),
    EpochIdMismatch,
    Fenced(GameModuleEpoch),
    FreeFailed {
        epoch: GameModuleEpoch,
        adoption: StoredAdoption,
    },
}

#[derive(Debug)]
struct EpochRecord {
    token: ModuleEpochToken,
    draining: bool,
    stored: BTreeSet<StoredAdoption>,
    descriptors: usize,
    placeholders: usize,
    plans: usize,
    fetches: usize,
}

#[derive(Debug, Default)]
pub struct RuntimeEpochs {
    records: BTreeMap<GameModuleEpoch, EpochRecord>,
}

impl RuntimeEpochs {
    pub fn register(
        &mut self,
        epoch: GameModuleEpoch,
        token: ModuleEpochToken,
        descriptors: usize,
    ) -> Result<(), RuntimeEpochError> {
        if epoch.0 != token.id() {
            return Err(RuntimeEpochError::EpochIdMismatch);
        }
        if self.records.contains_key(&epoch) {
            return Err(RuntimeEpochError::DuplicateEpoch(epoch));
        }
        self.records.insert(
            epoch,
            EpochRecord {
                token,
                draining: false,
                stored: BTreeSet::new(),
                descriptors,
                placeholders: 0,
                plans: 0,
                fetches: 0,
            },
        );
        Ok(())
    }

    pub fn can_issue_work(&self, epoch: GameModuleEpoch) -> bool {
        self.records
            .get(&epoch)
            .is_some_and(|record| !record.draining && !record.token.is_poisoned())
    }

    pub fn record_adoption(
        &mut self,
        epoch: GameModuleEpoch,
        adoption: StoredAdoption,
    ) -> Result<(), RuntimeEpochError> {
        let record = self
            .records
            .get_mut(&epoch)
            .ok_or(RuntimeEpochError::UnknownEpoch(epoch))?;
        if record.draining || record.token.is_poisoned() {
            return Err(RuntimeEpochError::Fenced(epoch));
        }
        record.stored.insert(adoption);
        Ok(())
    }

    pub fn record_placeholder(&mut self, epoch: GameModuleEpoch) -> Result<(), RuntimeEpochError> {
        self.resource(epoch, |record| record.placeholders += 1)
    }

    pub fn record_plan(&mut self, epoch: GameModuleEpoch) -> Result<(), RuntimeEpochError> {
        self.resource(epoch, |record| record.plans += 1)
    }

    pub fn record_fetch(&mut self, epoch: GameModuleEpoch) -> Result<(), RuntimeEpochError> {
        self.resource(epoch, |record| record.fetches += 1)
    }

    fn resource(
        &mut self,
        epoch: GameModuleEpoch,
        add: impl FnOnce(&mut EpochRecord),
    ) -> Result<(), RuntimeEpochError> {
        let record = self
            .records
            .get_mut(&epoch)
            .ok_or(RuntimeEpochError::UnknownEpoch(epoch))?;
        if record.draining || record.token.is_poisoned() {
            return Err(RuntimeEpochError::Fenced(epoch));
        }
        add(record);
        Ok(())
    }

    /// Fence new work immediately. Plans and in-flight fetches are failed and
    /// forgotten while module code is still resident; descriptors and
    /// placeholder thunks stay until all constructed values have been freed.
    pub fn begin_module_drain(&mut self, epoch: GameModuleEpoch) -> Result<(), RuntimeEpochError> {
        let record = self
            .records
            .get_mut(&epoch)
            .ok_or(RuntimeEpochError::UnknownEpoch(epoch))?;
        record.draining = true;
        record.plans = 0;
        record.fetches = 0;
        Ok(())
    }

    /// Run status-returning frees. All members are attempted even after one
    /// fails; a failure poisons the exact owner epoch and keeps that adoption
    /// accounted forever, preventing `dlclose`.
    pub fn drain(
        &mut self,
        epoch: GameModuleEpoch,
        storage: &mut dyn AssetStorage,
    ) -> Result<(), RuntimeEpochError> {
        let stored: Vec<_> = self
            .records
            .get(&epoch)
            .ok_or(RuntimeEpochError::UnknownEpoch(epoch))?
            .stored
            .iter()
            .copied()
            .collect();
        let mut first_failure = None;
        for adoption in stored {
            match storage.free(adoption.type_uuid, adoption.handle, adoption.adoption) {
                Ok(()) => {
                    self.records
                        .get_mut(&epoch)
                        .expect("epoch checked above")
                        .stored
                        .remove(&adoption);
                }
                Err(_) => {
                    let record = self.records.get_mut(&epoch).expect("epoch checked above");
                    record
                        .token
                        .poison_with(ModuleEpochPoisonCause::CallbackPanic);
                    first_failure.get_or_insert(adoption);
                }
            }
        }
        let record = self.records.get_mut(&epoch).expect("epoch checked above");
        if record.stored.is_empty() && !record.token.is_poisoned() {
            record.descriptors = 0;
            record.placeholders = 0;
        }
        if let Some(adoption) = first_failure {
            Err(RuntimeEpochError::FreeFailed { epoch, adoption })
        } else {
            Ok(())
        }
    }

    pub fn drain_complete(&self, epoch: GameModuleEpoch) -> bool {
        self.records.get(&epoch).is_some_and(|record| {
            record.draining
                && !record.token.is_poisoned()
                && record.stored.is_empty()
                && record.descriptors == 0
                && record.placeholders == 0
                && record.plans == 0
                && record.fetches == 0
        })
    }

    pub fn is_poisoned(&self, epoch: GameModuleEpoch) -> bool {
        self.records
            .get(&epoch)
            .is_some_and(|record| record.token.is_poisoned())
    }
}
