/*

Java's concurrent hashmap implementation in Rust

A hash table supporting full concurrency of gets (no blocks) and high expected
concurrency for updates (writes).

Every bin is basically a linked list of nodes.

*/


 /* ---------------- Constants -------------- */

/**
 * The largest possible table capacity.  This value must be
 * exactly 1<<30 to stay within Java array allocation and indexing
 * bounds for power of two table sizes, and is further required
 * because the top two bits of 32bit hash fields are used for
 * control purposes.
 */
const MAXIMUM_CAPACITY: usize = 1 << 30;

/**
 * The default initial table capacity.  Must be a power of 2
 * (i.e., at least 1) and at most MAXIMUM_CAPACITY.
 */
const DEFAULT_CAPACITY: usize = 16;

/**
 * The load factor for this table. Overrides of this value in
 * constructors affect only the initial table capacity.  The
 * actual floating point value isn't normally used -- it is
 * simpler to use expressions such as `n - (n >>> 2)` for
 * the associated resizing threshold.
 */
const LOAD_FACTOR: f64 = 0.75;

/**
 * Minimum number of rebinnings per transfer step. Ranges are
 * subdivided to allow multiple resizer threads.  This value
 * serves as a lower bound to avoid resizers encountering
 * excessive memory contention.  The value should be at least
 * DEFAULT_CAPACITY.
 */
const MIN_TRANSFER_STRIDE : usize = 16;

/**
 * The number of bits used for generation stamp in sizeCtl.
 * Must be at least 6 for 32bit arrays.
 */
const RESIZE_STAMP_BITS : usize = 16;

/**
 * The maximum number of threads that can help resize.
 * Must fit in 32 - RESIZE_STAMP_BITS bits.
 */
const MAX_RESIZERS : usize = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

/**
 * The bit shift for recording size stamp in sizeCtl.
 */
const RESIZE_STAMP_SHIFT : usize = 32 - RESIZE_STAMP_BITS;

// Number of CPUS, to place bounds on some sizings
// const NCPU : usize = Runtime.getRuntime().availableProcessors();


mod node;

// Crossbeam guards are ways to track one sequence of operations
// A guard that keeps the current thread pinned.
// When a guard gets dropped, the current thread is automatically unpinned.
// Having a guard allows us to create pointers on the stack to heap-allocated 
// objects. The guard is really just there to ensure that at some point you
// know that you are no longer holding any references into the target
// data structure.
// We can then combine this with a memory reclamation scheme because anytime
// the guard is dropped, all refs are also dropped and then therefore you may
// be able to do reclamation.
use crossbeam::epoch::{Atomic, Guard, Shared, Owned, CompareExchangeError};
use std::sync::atomic::Ordering;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use node::*;

// Main type
pub struct ConccHashMap<K, V, S=RandomState> {
    // When we resize, we will allocate a new table
    table: Atomic<Table<K, V>>,
    build_hasher: S,
}

impl <K, V, S> ConccHashMap<K, V, S> 
where 
    K: Hash,
    S: BuildHasher {

    fn hash(&self, key: &K) -> u64 {
        let mut h = self.build_hasher.build_hasher();
        // Give a mutable reference to the hasher
        key.hash(&mut h);
        // h is now the final hash for that key
        h.finish()
    }

    pub fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>>{
        let h = self.hash(key);
        // Read the table
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }
        // If there are no bins, then no key is present
        if table.bins.len() == 0 {
            return None;
        }

        // hash = 0b10010010011101111010111010
        // 4 bins == 0b100
        // mask = (4-1) == 0b011
        // take the low bits and index into the number of bins we have
        // hash & mask = 0b011 & 0b....010 -> 0b00000000000010

        let bini = table.bini(h);
        let bin = table.at(bini, guard);
        if bin.is_null() {
            return None;
        }
        let node = bin.find(h, key);
        if node.is_null() {
            return None;
        }

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        Some(v)
    }

    // pub fn get_and<R, F: FnOnce(&V) -> R>(&self, key:&K, then: F) -> Option<R> {
    //     let guard = &crossbeam::epoch::pin();
    //     self.get(key, guard).map(|v| then(&*v))
    // }


    // All these methods take an immutable reference to self.
    // Since this is a concurrent map
     pub fn insert(&self, key: K, value: V) -> Option<()> {}

     fn put(&self, key: K, value: V, if_absent: bool) -> Option<()> {
        let h = self.hash(&key);
        let mut binCount = 0;
        // As long as you are holding a guard, you are holding up the epochs
        // You are holding up the memory reclamation that might happen
        let guard = crossbeam::epoch::pin();
        let mut table = self.table.load(Ordering::SeqCst, &guard);
        loop {
            if table.is_null() || table.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }
            
            // Allocate a new node
            let mut node = Owned::new(BinEntry::Node {
                key,
                value,
                hash: h,
                next: Atomic::null(),
            });

            let bini = table.bini(h);
            let bin = table.bin(bini, guard);
            if bin.is_null() {
                // fast path
                // If bin is empty, we just need to create a new node and put
                // it in the front.
                match table.cas_bin(bini, bin, node, guard) {
                    Ok(_old_null_ptr) =>  {
                        // assert!(_old_null_ptr.is_null());
                        return None;
                    }
                    Err(changed) => {
                        // Restore node
                        assert!(!changed.current.is_null());
                        node = changed.new;
                        bin = changed.current;
                    }
                }
            }

            // Slow path
            // Bin is non-empty, need to link into it.
            

            // We will match on the bin

            match *bin {
                BinEntry::Moved(next_table) => {
                    table = table.help_transfer(next_table);
                    unimplemented!();
                }
                BinEntry::Node(ref head) 
                    if if_absent && head.hash == h && &head.key == &head.key => {
                        // Fast path if replacement is disallowed and 
                        // first bin  matches.
                        return Some(());
                }
                BinEntry::Node(ref head) => {
                    // Bin is non-empty, need to link into it, so we must
                    // take the lock.

                }
            }

        }
     }
}

struct Table<K, V> {
    // TODO: Inline this instead?
    // Atomic does an heap allocation
    bins: [Atomic<node::BinEntry<K, V>>]
}

impl<K, V> Table<K, V> {
    
    #[inline(always)]
    fn bini(&self, hash: u64) -> usize {
        let mask = self.bins.len() as u64 - 1;
        (hash & mask) as usize
    }

    #[inline]
    // Returns a ref to the ith element
    fn bin<'g>(&'g self, i: usize, guard: &'g Guard) 
                    -> Shared<'g, node::BinEntry<K, V>> {
        self.bins[i].load(Ordering::Acquire, guard)
    }

    // If cas_bin succeeds, then we get back a shared to the node 
    // that was removed. And if the node that was removed is not
    // empty, then we have to free it at some point.

    #[inline]
    // Owned is a value that you know that no one else has access to
    fn cas_bin<'g>(&'g self, i: usize, current: Shared<node::BinEntry<K, V>>,
            new: Owned<node::BinEntry<K, V>>,  guard: &'g Guard) 
                    -> Result<Shared<'g, node::BinEntry<K, V>>,
                    CompareExchangeError<'g, node::BinEntry<K, V>, Owned<node::BinEntry<K, V>>>> {
        // Something is currently the first bin and we know what 
        // the pointer to that is. That is current.
        // As long that has not been changed, we want to replace
        // it with the `new` bin entry.
        self.bins[i].compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire, guard)
    }
}
