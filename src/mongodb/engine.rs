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

    pub(crate) fn get_nested<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = doc;
        for (i, &part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                return current.get(part);
            }
            match current.get(part) {
                Some(Bson::Document(d)) => current = d,
                _ => return None,
            }
        }
        None
    }

    fn matches_doc(doc: &Document, filter: &Document) -> bool {
        if filter.is_empty() {
            return true;
        }
        for (k, v) in filter.iter() {
            if k == "$and" {
                if let Some(arr) = v.as_array() {
                    if !arr.iter().all(|cond| {
                        if let Bson::Document(cond_doc) = cond {
                            Self::matches_doc(doc, cond_doc)
                        } else {
                            false
                        }
                    }) {
                        return false;
                    }
                    continue;
                }
            } else if k == "$or" {
                if let Some(arr) = v.as_array() {
                    if !arr.iter().any(|cond| {
                        if let Bson::Document(cond_doc) = cond {
                            Self::matches_doc(doc, cond_doc)
                        } else {
                            false
                        }
                    }) {
                        return false;
                    }
                    continue;
                }
            } else if k == "$not"
                && let Bson::Document(cond_doc) = v
            {
                if Self::matches_doc(doc, cond_doc) {
                    return false;
                }
                continue;
            }

            let doc_v = Self::get_nested(doc, k);

            if let Bson::Document(op_doc) = v {
                // If it looks like a query operator doc (starts with $)
                if op_doc.keys().any(|key| key.starts_with('$')) {
                    for (op, op_val) in op_doc.iter() {
                        match op.as_str() {
                            "$eq" => {
                                if doc_v != Some(op_val) {
                                    return false;
                                }
                            }
                            "$ne" => {
                                if doc_v == Some(op_val) {
                                    return false;
                                }
                            }
                            "$gt" => {
                                if let Some(dv) = doc_v {
                                    if !Self::compare_bson(dv, op_val, |c| {
                                        c == std::cmp::Ordering::Greater
                                    }) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "$gte" => {
                                if let Some(dv) = doc_v {
                                    if !Self::compare_bson(dv, op_val, |c| {
                                        c == std::cmp::Ordering::Greater
                                            || c == std::cmp::Ordering::Equal
                                    }) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "$lt" => {
                                if let Some(dv) = doc_v {
                                    if !Self::compare_bson(dv, op_val, |c| {
                                        c == std::cmp::Ordering::Less
                                    }) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "$lte" => {
                                if let Some(dv) = doc_v {
                                    if !Self::compare_bson(dv, op_val, |c| {
                                        c == std::cmp::Ordering::Less
                                            || c == std::cmp::Ordering::Equal
                                    }) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "$in" => {
                                if let Bson::Array(arr) = op_val {
                                    if let Some(dv) = doc_v {
                                        if !arr.contains(dv) {
                                            return false;
                                        }
                                    } else {
                                        return false;
                                    }
                                }
                            }
                            "$nin" => {
                                if let Bson::Array(arr) = op_val
                                    && let Some(dv) = doc_v
                                    && arr.contains(dv)
                                {
                                    return false;
                                }
                            }
                            "$exists" => {
                                let exists = op_val.as_bool().unwrap_or(true);
                                if (doc_v.is_some()) != exists {
                                    return false;
                                }
                            }
                            _ => return false, // Unknown operator
                        }
                    }
                    continue;
                }
            }

            // Exact match
            if doc_v != Some(v) {
                return false;
            }
        }
        true
    }

    fn compare_bson<F>(a: &Bson, b: &Bson, f: F) -> bool
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        // Simple comparison for numbers and strings, ignoring cross-type complexities for now.
        if let (Some(a_f), Some(b_f)) = (Self::as_f64(a), Self::as_f64(b)) {
            if let Some(ord) = a_f.partial_cmp(&b_f) {
                return f(ord);
            }
        } else if let (Some(a_s), Some(b_s)) = (a.as_str(), b.as_str()) {
            return f(a_s.cmp(b_s));
        } else if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
            return f(a_i.cmp(&b_i));
        } else if let (Bson::Boolean(a_b), Bson::Boolean(b_b)) = (a, b) {
            return f(a_b.cmp(b_b));
        }
        false
    }

    fn as_f64(b: &Bson) -> Option<f64> {
        match b {
            Bson::Double(d) => Some(*d),
            Bson::Int32(i) => Some(*i as f64),
            Bson::Int64(i) => Some(*i as f64),
            _ => None,
        }
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

    pub fn aggregate(
        &self,
        db: &str,
        coll: &str,
        pipeline: &[Bson],
    ) -> Result<Vec<Document>, String> {
        let mut docs = if let Some(db_data) = self.data.get(db)
            && let Some(coll_data) = db_data.get(coll)
        {
            coll_data.clone()
        } else {
            vec![]
        };

        for stage_bson in pipeline {
            let stage = match stage_bson.as_document() {
                Some(d) => d,
                None => return Err("pipeline stage must be a document".to_string()),
            };
            if let Ok(match_doc) = stage.get_document("$match") {
                docs.retain(|d| Self::matches_doc(d, match_doc));
            } else if let Ok(project_doc) = stage.get_document("$project") {
                let mut new_docs = Vec::with_capacity(docs.len());
                let is_inclusion = project_doc.iter().any(|(k, v)| {
                    k != "_id" && (v.as_i32() == Some(1) || v.as_bool() == Some(true))
                });
                for doc in &docs {
                    let mut new_doc = Document::new();
                    // Include _id by default unless explicitly excluded
                    let include_id = project_doc
                        .get("_id")
                        .is_none_or(|v| v.as_i32() != Some(0) && v.as_bool() != Some(false));
                    if include_id && let Some(id) = doc.get("_id") {
                        new_doc.insert("_id", id.clone());
                    }

                    for (k, v) in project_doc.iter() {
                        if k == "_id" {
                            continue;
                        }
                        match v {
                            Bson::Int32(1)
                            | Bson::Int64(1)
                            | Bson::Double(1.0)
                            | Bson::Boolean(true) => {
                                if let Some(val) = Self::get_nested(doc, k) {
                                    new_doc.insert(k, val.clone());
                                }
                            }
                            Bson::Int32(0)
                            | Bson::Int64(0)
                            | Bson::Double(0.0)
                            | Bson::Boolean(false) => {
                                // Exclusion is handled below
                            }
                            Bson::String(field_path) if field_path.starts_with('$') => {
                                if let Some(val) = Self::get_nested(doc, &field_path[1..]) {
                                    new_doc.insert(k, val.clone());
                                }
                            }
                            _ => {} // Other expressions not supported yet
                        }
                    }

                    if !is_inclusion {
                        // If exclusion, copy everything not excluded
                        for (k, v) in doc.iter() {
                            if k == "_id" {
                                continue;
                            }
                            let exclude = project_doc.get(k).is_some_and(|pv| {
                                pv.as_i32() == Some(0) || pv.as_bool() == Some(false)
                            });
                            if !exclude {
                                new_doc.insert(k, v.clone());
                            }
                        }
                    }
                    new_docs.push(new_doc);
                }
                docs = new_docs;
            } else if let Ok(sort_doc) = stage.get_document("$sort") {
                docs.sort_by(|a, b| {
                    for (k, v) in sort_doc.iter() {
                        let a_val = Self::get_nested(a, k);
                        let b_val = Self::get_nested(b, k);
                        let asc = match v {
                            Bson::Int32(1) | Bson::Int64(1) | Bson::Double(1.0) => true,
                            Bson::Int32(-1) | Bson::Int64(-1) | Bson::Double(-1.0) => false,
                            _ => true,
                        };
                        let cmp = Self::compare_values(a_val, b_val);
                        if cmp != std::cmp::Ordering::Equal {
                            return if asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            } else if let Some(limit) = stage
                .get_i64("$limit")
                .ok()
                .or_else(|| stage.get_i32("$limit").ok().map(|i| i as i64))
            {
                docs.truncate(limit as usize);
            } else if let Some(skip) = stage
                .get_i64("$skip")
                .ok()
                .or_else(|| stage.get_i32("$skip").ok().map(|i| i as i64))
            {
                if skip as usize >= docs.len() {
                    docs.clear();
                } else {
                    docs.drain(0..skip as usize);
                }
            } else if let Some(unwind) = stage.get("$unwind") {
                let path = match unwind {
                    Bson::String(s) => s,
                    Bson::Document(d) => {
                        if let Ok(p) = d.get_str("path") {
                            p
                        } else {
                            return Err("$unwind missing path".to_string());
                        }
                    }
                    _ => return Err("$unwind must be string or document".to_string()),
                };
                if !path.starts_with('$') {
                    return Err("$unwind path must start with $".to_string());
                }
                let field_path = &path[1..];
                let mut new_docs = Vec::new();
                for doc in docs {
                    if let Some(Bson::Array(arr)) = Self::get_nested(&doc, field_path) {
                        for item in arr {
                            let mut new_doc = doc.clone();
                            // simple property replacement
                            let parts: Vec<&str> = field_path.split('.').collect();
                            let mut current = &mut new_doc;
                            for (i, &part) in parts.iter().enumerate() {
                                if i == parts.len() - 1 {
                                    current.insert(part, item.clone());
                                } else {
                                    // Needs proper mutable nesting handling, simplify for now
                                    if !current.contains_key(part) {
                                        break; // Cannot reach
                                    }
                                    let v = current.get_mut(part).unwrap();
                                    if let Bson::Document(d) = v {
                                        let ptr = d as *mut Document;
                                        unsafe {
                                            current = &mut *ptr;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                            new_docs.push(new_doc);
                        }
                    } else {
                        // ignore or preserve null (preserveNullAndEmptyArrays not implemented)
                    }
                }
                docs = new_docs;
            } else if let Ok(group_doc) = stage.get_document("$group") {
                let mut groups: std::collections::HashMap<String, Document> =
                    std::collections::HashMap::new();
                let id_expr = group_doc.get("_id");

                for doc in &docs {
                    // Evaluate _id
                    let mut group_key = Bson::Null;
                    if let Some(id_val) = id_expr {
                        if let Bson::String(s) = id_val {
                            if let Some(stripped) = s.strip_prefix('$') {
                                if let Some(v) = Self::get_nested(doc, stripped) {
                                    group_key = v.clone();
                                }
                            } else {
                                group_key = id_val.clone();
                            }
                        } else {
                            group_key = id_val.clone(); // Can be doc etc.
                        }
                    }
                    // Use a string representation as key for now
                    let key_str = format!("{:?}", group_key);

                    let group = groups.entry(key_str).or_insert_with(|| {
                        let mut d = Document::new();
                        d.insert("_id", group_key.clone());
                        d
                    });

                    // Evaluate accumulators
                    for (k, v) in group_doc.iter() {
                        if k == "_id" {
                            continue;
                        }
                        if let Bson::Document(acc) = v {
                            if let Some(sum_expr) = acc.get("$sum") {
                                let mut val_to_add = 0.0;
                                if let Bson::String(s) = sum_expr {
                                    if s.starts_with('$')
                                        && let Some(fv) = Self::get_nested(doc, &s[1..])
                                        && let Some(f) = Self::as_f64(fv)
                                    {
                                        val_to_add = f;
                                    }
                                } else if let Some(f) = Self::as_f64(sum_expr) {
                                    val_to_add = f;
                                }

                                let current =
                                    Self::as_f64(group.get(k).unwrap_or(&Bson::Double(0.0)))
                                        .unwrap_or(0.0);
                                group.insert(k, Bson::Double(current + val_to_add));
                            } else if let Some(avg_expr) = acc.get("$avg") {
                                // Simplified: tracking sum and count
                                let mut val_to_add = 0.0;
                                let mut has_val = false;
                                if let Bson::String(s) = avg_expr {
                                    if s.starts_with('$')
                                        && let Some(fv) = Self::get_nested(doc, &s[1..])
                                        && let Some(f) = Self::as_f64(fv)
                                    {
                                        val_to_add = f;
                                        has_val = true;
                                    }
                                } else if let Some(f) = Self::as_f64(avg_expr) {
                                    val_to_add = f;
                                    has_val = true;
                                }

                                if has_val {
                                    let state = group.get_document(k).ok();
                                    let current_sum =
                                        state.and_then(|d| d.get_f64("sum").ok()).unwrap_or(0.0);
                                    let current_count =
                                        state.and_then(|d| d.get_i64("count").ok()).unwrap_or(0);
                                    let mut new_state = Document::new();
                                    new_state.insert("sum", current_sum + val_to_add);
                                    new_state.insert("count", current_count + 1);
                                    group.insert(k, new_state);
                                }
                            } else if let Some(min_expr) = acc.get("$min") {
                                if let Bson::String(s) = min_expr
                                    && s.starts_with('$')
                                    && let Some(fv) = Self::get_nested(doc, &s[1..])
                                {
                                    let current = group.get(k);
                                    if current.is_none()
                                        || Self::compare_values(Some(fv), current)
                                            == std::cmp::Ordering::Less
                                    {
                                        group.insert(k, fv.clone());
                                    }
                                }
                            } else if let Some(max_expr) = acc.get("$max") {
                                if let Bson::String(s) = max_expr
                                    && s.starts_with('$')
                                    && let Some(fv) = Self::get_nested(doc, &s[1..])
                                {
                                    let current = group.get(k);
                                    if current.is_none()
                                        || Self::compare_values(Some(fv), current)
                                            == std::cmp::Ordering::Greater
                                    {
                                        group.insert(k, fv.clone());
                                    }
                                }
                            } else if let Some(_count_expr) = acc.get("$count") {
                                let current = group.get_i32(k).unwrap_or(0);
                                group.insert(k, current + 1);
                            }
                        }
                    }
                }

                // Finalize averages
                for group in groups.values_mut() {
                    for (k, v) in group_doc.iter() {
                        if k == "_id" {
                            continue;
                        }
                        if let Bson::Document(acc) = v
                            && acc.contains_key("$avg")
                            && let Ok(state) = group.get_document(k)
                        {
                            let sum = state.get_f64("sum").unwrap_or(0.0);
                            let count = state.get_i64("count").unwrap_or(1);
                            group.insert(k, sum / count as f64);
                        }
                    }
                }

                docs = groups.into_values().collect();
            } else {
                return Err(format!("unsupported pipeline stage: {:?}", stage.keys().next()));
            }
        }
        Ok(docs)
    }

    fn compare_values(a: Option<&Bson>, b: Option<&Bson>) -> std::cmp::Ordering {
        match (a, b) {
            (Some(av), Some(bv)) => {
                if let (Some(af), Some(bf)) = (Self::as_f64(av), Self::as_f64(bv)) {
                    af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
                } else if let (Some(astr), Some(bstr)) = (av.as_str(), bv.as_str()) {
                    astr.cmp(bstr)
                } else {
                    std::cmp::Ordering::Equal // Unimplemented comparison
                }
            }
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
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
