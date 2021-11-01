use crossbeam::epoch::{Atomic, Guard, Shared};
use std::sync::atomic::Ordering;

// Entry in a bin
// Will generally be `Node`. Any entry that is not first in the bin
// will be a node.
pub(crate) enum BinEntry<K, V> {
    Node(Node<K, V>),
    // Moved contains a const reference to the next table
    Moved(*const super::Table<K, V>),
}

// We don't require K to be hashable because the 
// hash already been computed
impl <K, V> BinEntry<K, V> 
where K: Eq,
{
    pub(crate) fn find<'g>(&'g self, hash: u64, key: &K, guard: &'g Guard) 
                -> Shared<'g, Node<K, V>> {
        match *self {
            BinEntry::Node(ref n) => {
                if n.hash == hash && &n.key == key {
                    return  Shared::from(n as *const _);
                }
                // Memory order acquire - 
                // If an atomic store in thread A is tagged 
                // memory_order_release and an atomic load in thread B 
                // from the same variable is tagged memory_order_acquire
                // once the atomic load is completed, 
                // thread B is guaranteed to see everything thread A wrote to memory.
                let next =  n.next.load(Ordering::SeqCst, guard);
                if next.is_null() {
                    return Shared::null();
                }
                return next.find(hash, key, guard);
            }
        }
    }
}

// The primary Node type with key-value entry
// We need a read-only ref to Node and still be able to mutate
// the value  
// Cells give the ability to have an immutable pointer or 
// reference to something and mutable pointer to the underlying data
pub(crate) struct Node<K, V> {
    pub(crate) hash: u64,
    pub(crate) key: K,
    // Use unsafecell for interior mutability
    pub(crate) value: Atomic<V>,
    // Next does not need to be option since Atomic can be null
    pub(crate) next: Atomic<BinEntry<K, V>>,
    // Lock
    pub(crate) lock: 
}