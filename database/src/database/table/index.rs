/// Action: Add
pub fn save_to_index() {
    unimplemented!()
}

/// Action: Update, moving from one index to another
pub fn update_index() {
    unimplemented!()
}

/// Action: Delete, removes item from index
pub fn remove_from_index() {
    unimplemented!()
}

/// For a given search field, return the items that match the search
pub fn get_from_index() {
    unimplemented!()
}

struct FullNameIndex {
    index: HashMap<String, Vec<EntityId>>,
}
