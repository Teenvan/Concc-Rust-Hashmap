use crossbeam::epoch::{Atomic, Guard, Shared};
use std::sync::atomic::Ordering;
use parking_lot::lock_api::Mutex;

// Entry in a bin
// Will generally be `Node`. Any entry that is not first in the bin
// will be a node.
pub(crate) enum BinEntry<K, V> {
    Node(Node<K, V>),
    Moved,
}

// We don't require K to be hashable because the 
// hash already been computed
impl <K, V> BinEntry<K, V> 
where K: Eq,
{
    // We want find to be implemented on binentry not on node
    // because if you hit something that is not a node, 
    // you want find to chain to somewhere else.
    pub(crate) fn find<'g>(&'g self, hash: u64, key: &K, guard: &'g Guard) 
                -> Shared<'g, Node<K, V>> {

        // Rust does not guarantee tail recursion
        // What is tail recursion ?
        /*
        In traditional recursion, the typical model is that you perform 
        your recursive calls first, and then you take the return value 
        of the recursive call and calculate the result. In this manner, you 
        don't get the result of your calculation until you have returned from 
        every recursive call.

        In tail recursion, you perform your calculations first, and then 
        you execute the recursive call, passing the results of your 
        current step to the next recursive step. This results in the 
        last statement being in the form of 
        (return (recursive-function params)). Basically, the return 
        value of any given recursive step is the same as the return 
        value of the next recursive call.

        The consequence of this is that once you are ready to perform 
        your next recursive step, you don't need the current stack frame 
        any more. This allows for some optimization. In fact, with an 
        appropriately written compiler, you should never have a 
        stack overflow snicker with a tail recursive call. Simply reuse 
        the current stack frame for the next recursive step. 
        */
        match *self {
            BinEntry::Node(ref n) => { 
                return n.find(hash, key, guard);
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
    // Use unsafecell for interior mutability (to give the ability to mutate
    // the node)
    // Cells are generally types that give you interior mutability ->
    // so the ability to have a immutable reference or pointer to something 
    // and a mutable pointer to the underlying data.
    pub(crate) value: Atomic<V>,
    // Next does not need to be option since Atomic can be null
    pub(crate) next: Atomic<Node<K, V>>,
    // Lock (parkinglog mutex is a lot smaller than the std lib)
    pub(crate) lock: Mutex<()>,
}


impl <K, V> Node<K, V> 
where K: Eq,
{
    pub(crate) fn find<'g>(&'g self, hash: u64, key: &K, guard: &'g Guard) 
                -> Shared<'g, Node<K, V>> {
        
        if self.hash == hash && &self.key == key {
            return  Shared::from(self as *const _);
        }
        // Memory order acquire - 
        // If an atomic store in thread A is tagged 
        // memory_order_release and an atomic load in thread B 
        // from the same variable is tagged memory_order_acquire
        // once the atomic load is completed, 
        // thread B is guaranteed to see everything thread A wrote to memory.
        let next =  self.next.load(Ordering::SeqCst, guard);
        if next.is_null() {
            return Shared::null();
        }
        return next.find(hash, key, guard);
    }
}