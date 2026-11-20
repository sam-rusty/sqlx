use std::fmt::{self, Debug, Formatter};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::ext::ustr::UStr;
use crate::types::Oid;
use crate::{HashMap, PgTypeInfo};

/// Type metadata resolved from `pg_type`, shared by connections to the same database.
///
/// Each connection keeps its own cache of the custom types it resolved (enums, domains,
/// composites, their arrays). Passing one `PgTypeCache` to several connections through
/// [`PgConnectOptions::shared_type_cache`][crate::PgConnectOptions::shared_type_cache]
/// seeds every new connection with what the others resolved, and every lookup a connection
/// makes is written back. A type is then looked up once per cache instead of once per
/// connection.
///
/// Use one cache per database: type OIDs are only meaningful in the database that
/// assigned them. A type dropped and created again gets a new OID, and connections
/// seeded with the old one fail with an error from the server until the cache is
/// [cleared][Self::clear].
#[derive(Default)]
pub struct PgTypeCache {
    maps: Mutex<TypeMaps>,
}

#[derive(Default)]
pub(crate) struct TypeMaps {
    pub(crate) type_oid: HashMap<UStr, Oid>,
    pub(crate) type_info: HashMap<Oid, PgTypeInfo>,
    pub(crate) elem_type_to_array: HashMap<Oid, Oid>,
}

impl PgTypeCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Forgets every type. Connections keep what they already copied.
    pub fn clear(&self) {
        *self.lock() = TypeMaps::default();
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, TypeMaps> {
        // the maps stay consistent across a panic: every write is a single insert
        self.maps.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Debug for PgTypeCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgTypeCache")
            .field("types", &self.lock().type_info.len())
            .finish()
    }
}

impl TypeMaps {
    /// Copies into `self` what `other` has and `self` does not. Returns whether anything
    /// was copied.
    pub(crate) fn merge_from(
        &mut self,
        type_oid: &HashMap<UStr, Oid>,
        type_info: &HashMap<Oid, PgTypeInfo>,
        elem_type_to_array: &HashMap<Oid, Oid>,
    ) -> bool {
        let before = self.len();
        for (name, oid) in type_oid {
            self.type_oid.entry_ref(name).or_insert(*oid);
        }
        for (oid, info) in type_info {
            self.type_info.entry(*oid).or_insert_with(|| info.clone());
        }
        for (elem, array) in elem_type_to_array {
            self.elem_type_to_array.entry(*elem).or_insert(*array);
        }
        self.len() != before
    }

    pub(crate) fn len(&self) -> usize {
        self.type_oid.len() + self.type_info.len() + self.elem_type_to_array.len()
    }
}
