// use std::{collections::HashMap, collections::HashSet};

// use crossbeam_skiplist::SkipMap;
// use serde::{Deserialize, Serialize};

// use crate::consts::consts::EntityId;

// /// Index captures values + null
// #[derive(Serialize, Deserialize, Debug)]
// pub struct FullNameIndex {
//     index: SkipMap<Option<String>, HashSet<EntityId>>,
// }

// impl Default for FullNameIndex {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// // TODO: Index features:
// //  1. Uniqueness constraints -- could this be implemented as a trait?
// //  2. Search not null (scan index?), aka. everything in the index is good except the null entries
// //  3. The whole &Option<String> thing is kind of wonky, because we need to pass in a cloned string in
// impl FullNameIndex {
//     pub fn new() -> Self {
//         Self {
//             index: HashMap::new(),
//         }
//     }

//     /// Statement: Add
//     pub fn save_to_index(&mut self, id: EntityId, full_name: Option<String>) {
//         match self.index.get_mut(&full_name) {
//             Some(ids) => {
//                 if ids.insert(id) == false {
//                     panic!("id should not be in index");
//                 }
//             }
//             None => {
//                 self.index.insert(full_name, HashSet::from([id]));
//             }
//         }
//     }

//     /// Statement: Update, moving from one index to another
//     pub fn update_index(
//         &mut self,
//         id: EntityId,
//         old_full_name: &Option<String>,
//         new_full_name: Option<String>,
//     ) {
//         // If they are the same there is no need to update indexes
//         if old_full_name == &new_full_name {
//             return;
//         }

//         self.remove_from_index(&id, old_full_name);
//         self.save_to_index(id, new_full_name);
//     }

//     /// Statement: Delete, removes item from index
//     pub fn remove_from_index(&mut self, id: &EntityId, full_name: &Option<String>) {
//         match self.index.get_mut(full_name) {
//             Some(ids) => {
//                 if ids.remove(id) == false {
//                     panic!("id should be in set");
//                 }
//             }
//             None => panic!("id should be in index"),
//         }
//     }

//     /// For a given search field, return the items that match the search
//     pub fn get_from_index(&self, full_name: &Option<String>) -> Option<&HashSet<EntityId>> {
//         self.index.get(full_name)
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;

//     // TODO: Make this test less 'co-pilot' like
//     #[test]
//     fn test_full_name_index() {
//         let mut index = FullNameIndex::new();

//         let id1 = EntityId("1".to_string());
//         let id2 = EntityId("2".to_string());
//         let id3 = EntityId("3".to_string());

//         let full_name1 = Some("Full Name 1".to_string());
//         let full_name2 = Some("Full Name 2".to_string());
//         let full_name3 = Some("Full Name 3".to_string());

//         index.save_to_index(id1.clone(), full_name1.clone());
//         index.save_to_index(id2.clone(), full_name2.clone());
//         index.save_to_index(id3.clone(), full_name3.clone());

//         assert_eq!(
//             index.get_from_index(&full_name1),
//             Some(&HashSet::from([id1.clone()]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name2),
//             Some(&HashSet::from([id2.clone()]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name3),
//             Some(&HashSet::from([id3.clone()]))
//         );

//         index.update_index(id1.clone(), &full_name1, full_name2.clone());

//         assert_eq!(
//             index.get_from_index(&full_name1),
//             Some(&HashSet::from([] as [EntityId; 0]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name2),
//             Some(&HashSet::from([id1.clone(), id2.clone()]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name3),
//             Some(&HashSet::from([id3.clone()]))
//         );

//         index.remove_from_index(&id1, &full_name2);

//         assert_eq!(
//             index.get_from_index(&full_name1),
//             Some(&HashSet::from([] as [EntityId; 0]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name2),
//             Some(&HashSet::from([id2.clone()]))
//         );
//         assert_eq!(
//             index.get_from_index(&full_name3),
//             Some(&HashSet::from([id3.clone()]))
//         );
//     }
// }
