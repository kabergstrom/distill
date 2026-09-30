//! Lineage-repair inspection: the exact missing/duplicate lineage basis a
//! repair capability may act on, and the checks on a repair manifest.

use distill_json::AuthoredValue;

use crate::*;

pub(crate) enum LineageInspectionFailure {
    Unavailable(LineageRepairUnavailable),
    Invalid(RpcFailure),
}

pub(crate) fn lineage_inspection(
    stamp: SnapshotStamp,
    configuration: &ConfigurationStatus,
    repair: Option<&LineageRepairState>,
) -> Result<LineageRepairInspection, LineageInspectionFailure> {
    let instance = stamp.instance;
    match configuration {
        ConfigurationStatus::Ready => Err(LineageInspectionFailure::Unavailable(
            LineageRepairUnavailable::ConfigurationReady,
        )),
        ConfigurationStatus::Poisoned(poison) => match poison.detail.as_ref() {
            DscpV1::MissingLineageManifest => match repair {
                Some(state @ LineageRepairState::Missing { .. }) => Ok(LineageRepairInspection {
                    instance,
                    stamp,
                    state: state.clone(),
                }),
                _ => Err(LineageInspectionFailure::Invalid(
                    RpcFailure::InvalidAuthoringRequest {
                        detail: "missing-lineage DSCP has no exact repair inspection".to_owned(),
                    },
                )),
            },
            DscpV1::DuplicateLineageManifest { entries } => match repair {
                Some(state @ LineageRepairState::Duplicate { claimants })
                    if claimants == entries =>
                {
                    Ok(LineageRepairInspection {
                        instance,
                        stamp,
                        state: state.clone(),
                    })
                }
                _ => Err(LineageInspectionFailure::Invalid(
                    RpcFailure::InvalidAuthoringRequest {
                        detail: "duplicate-lineage DSCP disagrees with repair claimants".to_owned(),
                    },
                )),
            },
            _ => Err(LineageInspectionFailure::Unavailable(
                LineageRepairUnavailable::OtherConfigurationPoison(poison.clone()),
            )),
        },
    }
}

pub(crate) fn lineage_stale(
    basis: &LineageRepairInspection,
    current: &LineageRepairInspection,
) -> LineageRepairStale {
    let code = if basis.state == current.state {
        LineageRepairStaleCode::StampChanged
    } else {
        match (&basis.state, &current.state) {
            (
                LineageRepairState::Missing {
                    destination: LineageRepairDestination::Absent,
                    ..
                },
                LineageRepairState::Missing {
                    destination: LineageRepairDestination::Occupied { .. },
                    ..
                },
            ) => LineageRepairStaleCode::DestinationAppeared,
            (LineageRepairState::Missing { .. }, LineageRepairState::Missing { .. }) => {
                LineageRepairStaleCode::PreimageChanged
            }
            (LineageRepairState::Duplicate { .. }, LineageRepairState::Duplicate { .. }) => {
                LineageRepairStaleCode::ClaimantChanged
            }
            _ => LineageRepairStaleCode::StateChanged,
        }
    };
    LineageRepairStale {
        code,
        observed_stamp: current.stamp,
    }
}

pub(crate) fn validate_manifest_repair_bundle(bytes: &[u8]) -> Result<(), LineageRepairInvalid> {
    let invalid = |code, message: &str| LineageRepairInvalid {
        code,
        message: message.to_owned(),
    };
    let bundle = distill_bundle::parse_bundle(bytes).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            &format!("manifest bundle does not parse canonically: {error}"),
        )
    })?;
    let canonical = distill_bundle::write_bundle(&bundle).map_err(|error| {
        invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            &format!("manifest bundle cannot be rendered canonically: {error}"),
        )
    })?;
    if canonical != bytes {
        return Err(invalid(
            LineageRepairInvalidCode::NonCanonicalBundle,
            "manifest bundle bytes are not the canonical encoding",
        ));
    }
    let mut manifests = bundle.assets.values().filter(|entry| {
        entry.type_uuid == distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID
    });
    let Some(manifest) = manifests.next() else {
        return Err(invalid(
            LineageRepairInvalidCode::MissingManifestEntry,
            "bundle contains no SchemaLineageManifest entry",
        ));
    };
    if manifests.next().is_some() {
        return Err(invalid(
            LineageRepairInvalidCode::MissingManifestEntry,
            "bundle must contain exactly one SchemaLineageManifest entry",
        ));
    }
    if !manifest.authoring_only {
        return Err(invalid(
            LineageRepairInvalidCode::NotAuthoringOnly,
            "SchemaLineageManifest entry must be authoring_only",
        ));
    }
    if !matches!(
        manifest.lineage,
        distill_bundle::EntryLineageV1::Bootstrap {
            bundle_format_version: 1
        }
    ) {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest entry must use format-v1 bootstrap lineage",
        ));
    }
    let authority =
        distill_core::bootstrap::bootstrap_control_logical_registry_v1().map_err(|error| {
            invalid(
                LineageRepairInvalidCode::InvalidLineage,
                &format!("bootstrap authority unavailable: {error}"),
            )
        })?;
    let expected = authority
        .get(&distill_core::bootstrap::SCHEMA_LINEAGE_MANIFEST_TYPE_UUID)
        .expect("logical bootstrap authority contains lineage manifest");
    if manifest.schema_hash != *expected {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest entry has the wrong sealed logical schema",
        ));
    }
    let Some(type_keys) = lineage_manifest_type_keys(&manifest.data) else {
        return Err(invalid(
            LineageRepairInvalidCode::InvalidLineage,
            "SchemaLineageManifest data does not contain its canonical types map",
        ));
    };
    if type_keys
        .iter()
        .any(|type_uuid| distill_core::bootstrap::is_bootstrap_control_type(*type_uuid))
    {
        return Err(invalid(
            LineageRepairInvalidCode::BootstrapTypePresent,
            "SchemaLineageManifest types map contains a bootstrap-control TypeUuid",
        ));
    }
    Ok(())
}

pub(crate) fn lineage_manifest_type_keys(value: &AuthoredValue) -> Option<Vec<TypeUuid>> {
    let AuthoredValue::Object(fields) = value else {
        return None;
    };
    let AuthoredValue::Array(rows) = fields.get("types")? else {
        return None;
    };
    rows.iter()
        .map(|row| {
            let AuthoredValue::Array(pair) = row else {
                return None;
            };
            let [key, _value] = pair.as_slice() else {
                return None;
            };
            let AuthoredValue::Array(bytes) = key else {
                return None;
            };
            if bytes.len() != 16 {
                return None;
            }
            let mut uuid = [0; 16];
            for (output, byte) in uuid.iter_mut().zip(bytes) {
                let AuthoredValue::UInt(byte) = byte else {
                    return None;
                };
                *output = u8::try_from(*byte).ok()?;
            }
            Some(TypeUuid(uuid))
        })
        .collect()
}
