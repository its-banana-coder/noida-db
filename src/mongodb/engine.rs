use bson::{Bson, Document, doc, oid::ObjectId};
use std::collections::HashMap;

#[derive(Default)]
pub struct Engine {
    // db_name -> (collection_name -> documents)
    #[allow(clippy::type_complexity)]
    data: HashMap<String, HashMap<String, Vec<Document>>>,
    #[allow(clippy::type_complexity)]
    indexes: HashMap<String, HashMap<String, HashMap<String, (Document, bool)>>>,
}

impl Engine {
    pub fn new() -> Self {
        Self { data: HashMap::new(), indexes: HashMap::new() }
    }

    pub fn insert(&mut self, db: &str, coll: &str, mut docs: Vec<Document>) -> Result<i32, String> {
        let mut inserted = 0;
        for doc in docs.iter_mut() {
            if !doc.contains_key("_id") {
                doc.insert("_id", ObjectId::new());
            }

            {
                let coll_data = self.data.get(db).and_then(|d| d.get(coll));
                let coll_data_ref = coll_data.map(|v| v.as_slice()).unwrap_or(&[]);
                if let Err(e) = self.check_unique_indexes(db, coll, doc, coll_data_ref) {
                    if inserted == 0 {
                        return Err(e);
                    }
                    break; // Stop on first error for ordered inserts
                }
            }

            let db_data = self.data.entry(db.to_string()).or_default();
            let coll_data = db_data.entry(coll.to_string()).or_default();
            coll_data.push(doc.clone());
            inserted += 1;
        }

        Ok(inserted)
    }

    pub fn find(&self, db: &str, coll: &str, filter: &Document) -> Vec<Document> {
        if let Some(db_data) = self.data.get(db)
            && let Some(coll_data) = db_data.get(coll)
        {
            return coll_data.iter().filter(|d| Self::matches_doc(d, filter)).cloned().collect();
        }
        vec![]
    }

    pub fn delete(&mut self, db: &str, coll: &str, filter: &Document, limit: i32) -> i32 {
        if let Some(db_data) = self.data.get_mut(db)
            && let Some(coll_data) = db_data.get_mut(coll)
        {
            let mut deleted = 0;
            let mut i = 0;
            while i < coll_data.len() {
                if Self::matches_doc(&coll_data[i], filter) {
                    coll_data.remove(i);
                    deleted += 1;
                    if limit == 1 && deleted == 1 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            return deleted;
        }
        0
    }

    pub fn update(
        &mut self,
        db: &str,
        coll: &str,
        filter: &Document,
        update: &Document,
        multi: bool,
        upsert: bool,
    ) -> (i32, i32) {
        let db_data = self.data.entry(db.to_string()).or_default();
        let coll_data = db_data.entry(coll.to_string()).or_default();

        let mut matched = 0;
        let mut modified = 0;

        for doc in coll_data.iter_mut() {
            if Self::matches_doc(doc, filter) {
                matched += 1;
                if Self::apply_update_doc(doc, update) {
                    modified += 1;
                }
                if !multi {
                    break;
                }
            }
        }

        if matched == 0 && upsert {
            let mut new_doc = filter.clone();
            if !new_doc.contains_key("_id") {
                new_doc.insert("_id", ObjectId::new());
            }
            Self::apply_update_doc(&mut new_doc, update);
            coll_data.push(new_doc);
            return (0, 1); // 0 matched, 1 modified (upserted)
        }

        (matched, modified)
    }

    fn matches_doc(doc: &Document, filter: &Document) -> bool {
        if filter.is_empty() {
            return true;
        }
        for (k, v) in filter.iter() {
            if let Some(doc_v) = doc.get(k) {
                if doc_v != v {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn apply_update_doc(doc: &mut Document, update: &Document) -> bool {
        let mut modified = false;
        if let Some(Bson::Document(set_doc)) = update.get("$set") {
            for (k, v) in set_doc.iter() {
                doc.insert(k, v.clone());
                modified = true;
            }
        }
        // Replacement document (if no $set, $unset, etc)
        if !update.keys().any(|k| k.starts_with('$')) {
            let id = doc.get("_id").cloned();
            *doc = update.clone();
            if let Some(id) = id {
                doc.insert("_id", id);
            }
            modified = true;
        }
        modified
    }

    pub fn create_index(
        &mut self,
        db: &str,
        coll: &str,
        name: &str,
        keys: Document,
        unique: bool,
    ) -> Result<(), String> {
        let db_indexes = self.indexes.entry(db.to_string()).or_default();
        let coll_indexes = db_indexes.entry(coll.to_string()).or_default();

        if unique
            && let Some(db_data) = self.data.get(db)
            && let Some(coll_data) = db_data.get(coll)
        {
            // Check for existing duplicates
            let mut seen = std::collections::HashSet::new();
            for doc in coll_data {
                let mut key_vals = Vec::new();
                for k in keys.keys() {
                    key_vals.push(doc.get(k).cloned());
                }
                // Only enforce uniqueness if fields exist (sparse by default behavior for now)
                if key_vals.iter().all(Option::is_some) && !seen.insert(format!("{:?}", key_vals)) {
                    return Err(format!(
                        "E11000 duplicate key error collection: {}.{} index: {} dup key: {{ ... }}",
                        db, coll, name
                    ));
                }
            }
        }

        coll_indexes.insert(name.to_string(), (keys, unique));
        Ok(())
    }

    pub fn check_unique_indexes(
        &self,
        db: &str,
        coll: &str,
        doc: &Document,
        coll_data: &[Document],
    ) -> Result<(), String> {
        if let Some(db_indexes) = self.indexes.get(db)
            && let Some(coll_indexes) = db_indexes.get(coll)
        {
            for (name, (keys, unique)) in coll_indexes.iter() {
                if !*unique {
                    continue;
                }
                let mut filter = doc! {};
                let mut has_keys = true;
                for k in keys.keys() {
                    if let Some(v) = doc.get(k) {
                        filter.insert(k, v.clone());
                    } else {
                        has_keys = false;
                        break;
                    }
                }
                if !has_keys {
                    continue;
                }
                // Check if this filter matches any existing document
                let existing_id = doc.get("_id");
                for existing_doc in coll_data {
                    // Don't check against self on update
                    if let Some(e_id) = existing_doc.get("_id")
                        && let Some(d_id) = existing_id
                        && e_id == d_id
                    {
                        continue;
                    }

                    if Self::matches_doc(existing_doc, &filter) {
                        return Err(format!(
                            "E11000 duplicate key error collection: {}.{} index: {} dup key: {:?}",
                            db, coll, name, filter
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_find() {
        let mut eng = Engine::new();
        eng.insert("test", "c1", vec![doc! {"a": 1}]).unwrap();
        let res = eng.find("test", "c1", &doc! {"a": 1});
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].get("a").unwrap().as_i32().unwrap(), 1);
        assert!(res[0].contains_key("_id"));
    }
}
