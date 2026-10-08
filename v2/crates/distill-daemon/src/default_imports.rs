//! Default imports (§8 "Default imports"): the import rules a pipeline module
//! registers by path, applied under every asset root as the lowest layer
//! below explicit imports and authored rules bundles.
//!
//! The layer of one compiled state is its pipeline's default rules plus, per
//! configured root, the root's reserved rules-bundle UUID (what a default
//! import's `DirectoryOrigin` names) and the paths its configuration
//! excludes. It exists only while the pipeline is Ready: without one the
//! pass neither imports by default nor orphans what defaults imported.

use std::collections::BTreeMap;

use distill_build::import::{DefaultImportRule, NO_IMPORTER};
use distill_build::query::{normalize_identifier, FileQuery};
use distill_core::id::{default_rules_bundle, AssetUuid, BundleUuid};
use globset::{Glob, GlobMatcher, GlobSet, GlobSetBuilder};

use crate::importer::RegisteredImporters;
use crate::scanner::AssetRoot;

/// The default import layer of one compiled state.
pub(crate) struct DefaultImports {
    /// In rule-id order.
    pub(crate) rules: Vec<DefaultRule>,
    /// In root-name order.
    pub(crate) roots: Vec<DefaultRoot>,
}

pub(crate) struct DefaultRule {
    pub(crate) rule: DefaultImportRule,
    matches: PathMatcher,
}

pub(crate) struct DefaultRoot {
    pub(crate) name: String,
    pub(crate) rules_bundle: BundleUuid,
    excludes: GlobSet,
}

impl DefaultImports {
    /// The layer of a Ready pipeline registering `rules` with `importers`,
    /// over `roots` with the configured `excludes`. Every rule's importer
    /// must be registered.
    pub(crate) fn prepare(
        rules: Vec<DefaultImportRule>,
        importers: &RegisteredImporters,
        roots: &[AssetRoot],
        excludes: &BTreeMap<String, Vec<String>>,
    ) -> Result<Self, String> {
        let mut prepared = Vec::with_capacity(rules.len());
        for rule in rules {
            validate_rule(&rule)?;
            if !importers.contains_key(&rule.importer) {
                return Err(format!(
                    "default import rule {} names importer {:?}, which the pipeline does not register",
                    AssetUuid(rule.id.0),
                    rule.importer
                ));
            }
            let matches = PathMatcher::new(&rule.matches)?;
            prepared.push(DefaultRule { rule, matches });
        }
        prepared.sort_by(|left, right| left.rule.id.cmp(&right.rule.id));
        let mut layer_roots = Vec::with_capacity(roots.len());
        for root in roots {
            let mut builder = GlobSetBuilder::new();
            for pattern in excludes.get(&root.name).into_iter().flatten() {
                builder.add(Glob::new(pattern).map_err(|error| {
                    format!(
                        "default import exclude {pattern:?} of root {:?}: {error}",
                        root.name
                    )
                })?);
            }
            layer_roots.push(DefaultRoot {
                name: root.name.clone(),
                rules_bundle: default_rules_bundle(&root.name),
                excludes: builder.build().map_err(|error| error.to_string())?,
            });
        }
        layer_roots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Self {
            rules: prepared,
            roots: layer_roots,
        })
    }

    pub(crate) fn root(&self, name: &str) -> Option<&DefaultRoot> {
        self.roots.iter().find(|root| root.name == name)
    }

    pub(crate) fn rule(&self, index: usize) -> &DefaultRule {
        &self.rules[index]
    }

    /// Whether `id` is a default rule's: authored rules may not reuse it.
    pub(crate) fn has_rule(&self, id: &[u8; 16]) -> bool {
        self.rules.iter().any(|rule| rule.rule.id.0 == *id)
    }
}

impl DefaultRule {
    pub(crate) fn matches(&self, path: &str) -> bool {
        self.matches.matches(path)
    }
}

impl DefaultRoot {
    pub(crate) fn excludes(&self, path: &str) -> bool {
        self.excludes.is_match(path)
    }
}

/// The root whose default layer `rules_bundle` is the reserved UUID of,
/// among `roots`.
pub(crate) fn default_layer_root(roots: &[AssetRoot], rules_bundle: BundleUuid) -> Option<&str> {
    roots
        .iter()
        .find(|root| default_rules_bundle(&root.name) == rules_bundle)
        .map(|root| root.name.as_str())
}

/// A `FileQuery`, compiled once: `query_matches` without a glob compile per
/// call.
pub(crate) struct PathMatcher {
    prefix: Option<String>,
    glob: Option<GlobMatcher>,
}

impl PathMatcher {
    pub(crate) fn new(query: &FileQuery) -> Result<Self, String> {
        Ok(Self {
            prefix: query.path_prefix.clone(),
            glob: query
                .path_glob
                .as_deref()
                .map(|pattern| {
                    Glob::new(pattern)
                        .map(|glob| glob.compile_matcher())
                        .map_err(|error| format!("invalid glob {pattern:?}: {error}"))
                })
                .transpose()?,
        })
    }

    pub(crate) fn matches(&self, path: &str) -> bool {
        let prefix_matches = self.prefix.as_ref().is_none_or(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        });
        prefix_matches && self.glob.as_ref().is_none_or(|glob| glob.is_match(path))
    }
}

/// A default rule's own validity, before the epoch it is registered in is
/// complete.
pub(crate) fn validate_rule(rule: &DefaultImportRule) -> Result<(), String> {
    let id = AssetUuid(rule.id.0);
    if normalize_identifier(&rule.importer).ok().as_deref() != Some(rule.importer.as_str()) {
        return Err(format!(
            "default import rule {id} names importer {:?}, which is not a normalized identifier",
            rule.importer
        ));
    }
    if rule.importer == NO_IMPORTER {
        return Err(format!(
            "default import rule {id} names the reserved importer {NO_IMPORTER:?}"
        ));
    }
    if rule.matches.path_prefix.is_none() && rule.matches.path_glob.is_none() {
        return Err(format!("default import rule {id} matches no path selector"));
    }
    PathMatcher::new(&rule.matches)
        .map_err(|error| format!("default import rule {id}: {error}"))?;
    if rule.output.is_empty() || rule.output.contains(['/', '\\']) {
        return Err(format!(
            "default import rule {id}: output template {:?} must render one file name",
            rule.output
        ));
    }
    Ok(())
}

/// Whether two default rules may match one file: a path built from either
/// rule's selectors (alternatives expanded, wildcards filled) that the other
/// matches. Exact for extension rules such as `**/*.{png,jpg}`; a pass still
/// refuses a file two rules match.
pub(crate) fn rules_overlap(left: &DefaultImportRule, right: &DefaultImportRule) -> bool {
    let (Ok(left_matcher), Ok(right_matcher)) = (
        PathMatcher::new(&left.matches),
        PathMatcher::new(&right.matches),
    ) else {
        return false;
    };
    sample_paths(&left.matches)
        .iter()
        .any(|path| left_matcher.matches(path) && right_matcher.matches(path))
        || sample_paths(&right.matches)
            .iter()
            .any(|path| right_matcher.matches(path) && left_matcher.matches(path))
}

/// Paths `query` is meant to match: its glob's alternatives expanded and its
/// wildcards filled, also under its prefix.
fn sample_paths(query: &FileQuery) -> Vec<String> {
    let mut expanded = Vec::new();
    expand_alternatives(query.path_glob.as_deref().unwrap_or("x"), &mut expanded);
    let mut paths = Vec::new();
    for pattern in expanded {
        let filled = fill_wildcards(&pattern);
        if let Some(prefix) = &query.path_prefix {
            paths.push(format!("{prefix}/{filled}"));
        }
        paths.push(filled);
    }
    paths
}

/// `pattern` with each `{a,b}` group expanded, at most 256 patterns.
fn expand_alternatives(pattern: &str, out: &mut Vec<String>) {
    if out.len() >= 256 {
        return;
    }
    let Some(open) = pattern.find('{') else {
        out.push(pattern.to_owned());
        return;
    };
    let mut depth = 0;
    let mut close = None;
    let mut splits = Vec::new();
    for (index, byte) in pattern.bytes().enumerate().skip(open) {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(index);
                    break;
                }
            }
            b',' if depth == 1 => splits.push(index),
            _ => {}
        }
    }
    let Some(close) = close else {
        out.push(pattern.to_owned());
        return;
    };
    let mut start = open + 1;
    for end in splits.into_iter().chain(std::iter::once(close)) {
        let candidate = format!(
            "{}{}{}",
            &pattern[..open],
            &pattern[start..end],
            &pattern[close + 1..]
        );
        expand_alternatives(&candidate, out);
        start = end + 1;
    }
}

/// `pattern` with `**/` dropped and every other wildcard or class replaced by
/// one literal character.
fn fill_wildcards(pattern: &str) -> String {
    let pattern = pattern.replace("**/", "");
    let mut filled = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => {
                while chars.peek() == Some(&'*') {
                    chars.next();
                }
                filled.push('x');
            }
            '?' => filled.push('x'),
            '[' => {
                let mut first = None;
                for class in chars.by_ref() {
                    if class == ']' {
                        break;
                    }
                    if first.is_none() && class != '!' && class != '^' {
                        first = Some(class);
                    }
                }
                filled.push(first.unwrap_or('x'));
            }
            '\\' => filled.extend(chars.next()),
            c => filled.push(c),
        }
    }
    filled
}

#[cfg(test)]
mod tests {
    use super::*;
    use distill_build::import::ImportRuleId;
    use distill_build::trace::DirectoryGrouping;

    fn rule(id: u8, glob: &str) -> DefaultImportRule {
        DefaultImportRule {
            id: ImportRuleId([id; 16]),
            matches: FileQuery::new(None, Some(glob.to_owned())).unwrap(),
            group: DirectoryGrouping::PerFile,
            importer: "test.importer".to_owned(),
            output: "{name}.bundle".to_owned(),
        }
    }

    #[test]
    fn extension_rules_overlap_exactly_when_they_share_an_extension() {
        let images = rule(1, "**/*.{png,jpg,jpeg}");
        assert!(rules_overlap(&images, &rule(2, "**/*.jpg")));
        assert!(rules_overlap(&rule(2, "**/*.jpg"), &images));
        assert!(rules_overlap(&images, &rule(3, "textures/**/*.png")));
        assert!(!rules_overlap(&images, &rule(4, "**/*.{glb,gltf}")));
        assert!(!rules_overlap(&images, &rule(5, "**/*.dds")));
        assert!(!rules_overlap(
            &rule(6, "**/*.{vert,frag,comp}"),
            &rule(7, "**/*.{wav,ogg,flac}")
        ));
    }

    #[test]
    fn a_rule_is_validated_on_its_own() {
        assert!(validate_rule(&rule(1, "**/*.png")).is_ok());
        let mut none = rule(1, "**/*.png");
        none.importer = NO_IMPORTER.to_owned();
        assert!(validate_rule(&none).is_err());
        let mut nested = rule(1, "**/*.png");
        nested.output = "out/{name}.bundle".to_owned();
        assert!(validate_rule(&nested).is_err());
    }
}
