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
use parking_lot::lock_api::Mutex;
use std::sync::atomic::{AtomicUsize, AtomicIsize};

// Main type
pub struct ConccHashMap<K, V, S=RandomState> {
    // When we resize, we will allocate a new table
    // This is the base of the map
    // We need a notion of a table that is separate from 
    // the notion of a hashmap.
    /// The array of bins. Lazily initialized upon first insertion.
    /// Size is always a power of 2. Accessed directly by iterators. 
    table: Atomic<Table<K, V, S>>,
    /// The next table to use; non-null only while resizing. 
    next_table: Atomic<Table<K, V, S>>,
    /// The next table index (plus one) to split while resizing. 
    transfer_index: AtomicIsize,
    build_hasher: S,
    count: AtomicUsize,
    /*
     * Table initialization and resizing control. When negative, the
     * table is being initialized or resized: -1 for initialization,
     * else  -(1 + the number of active resizing threads). Otherwise,
     * when table is null, holds the initial table size to use upon 
     * creation, or 0 for default. After initialization, holds the 
     * next element count value upon which to resize the table.
     */ 
    size_ctl: AtomicIsize
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

    fn add_count(&self, n: isize, resize_hint: Option<usize>) {
        
        // TODO: Implement the java Countercell business here

        let count = if n > 0 {
            self.count.fetch_add(n as usize, Ordering::SeqCst)
        } else if n < 0 {
            let n = n.abs() as usize;
            self.count.fetch_sub(n, Ordering::SeqCst) - n
        } else {
            self.count.load(Ordering::SeqCst)
        };

        // if resize_hint is None, it means the caller does not want us to
        // consider a resize
        // If it is Some(n), the caller saw n entries in a bin

        if resize_hint.is_none() {
            return;
        }

        let saw_bin_length = resize_hint.unwrap();

        loop {
            let sc = self.size_ctl.load(Ordering::SeqCst);

            if count < sc {
                // Do nothing since we are not at the next resize point yet 
                break;
            }

            // Read the table
            let guard = crossbeam::epoch::pin();
            let mut table = self.table.load(Ordering::SeqCst, &guard);

            if table.is_null() {
                // table will be initialized by another thread anyway
                break;
            }
            
            let n = table.bins.len();
            if n >= MAXIMUM_CAPACITY {
                // cannot resize any more anyway
                break;
            }

            let rs = Self::resize_stamp(n) << RESIZE_STAMP_SHIFT;
            if sc < 0 {
                // Ongoing resize
                // There is already a resize happening
                // Can we join the resize transfer ?
                if sc == rs + MAX_RESIZERS || sc == rs + 1 {
                    break;
                }

                let nt = self.next_table.load(Ordering::SeqCst, &guard);
                if nt.is_null() {
                    break;
                }

                if self.transfer_index.load(Ordering::SeqCst) <= 0 {
                    break;
                }

                // Try to join the resize transfer
                if self.size_ctl.compare_exchange(sc, sc+1, Ordering::SeqCst, Ordering::Acquire) == sc {
                    self.transfer(table, nt);
                }
            } else if self.size_ctl.compare_exchange(sc, rs + 2, Ordering::SeqCst, Ordering::Acquire) == sc {
                // a resize is needed, but has not yet started
                // There is no new table yet
                self.transfer(table, Atomic::null());
            }

            // another resize maybe needed
            count = self.count.load(Ordering::SeqCst); 
        }
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
        // 4 bins == 0b100 (Binary representation of 4)
        // mask = (4-1) == 0b011
        // take the low bits and index into the number of bins we have
        // hash & mask = 0b011 & 0b....010 -> 0b00000000000010

        let bini = table.bini(h);
        let bin = table.at(bini, guard);

        // If the map is being destroyed
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

     fn put(&self, key: K, value: V, no_replacement: bool) -> Option<()> {
        let h = self.hash(&key);
        // let mut bin_count = 0;
        // As long as you are holding a guard, you are holding up the epochs
        // You are holding up the memory reclamation that might happen
        let guard = crossbeam::epoch::pin();
        let mut table = self.table.load(Ordering::SeqCst, &guard);
        // Allocate a new node
        let mut node = Owned::new(BinEntry::Node {
            key,
            value,
            hash: h,
            next: Atomic::null(),
        });

        loop {
            if table.is_null() || table.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }

            let bini = table.bini(h);
            let bin = table.bin(bini, guard);
            if bin.is_null() {
                // fast path
                // If bin is empty, we just need to create a new node and put
                // it in the front.
                match table.cas_bin(bini, bin, node, guard) {
                    Ok(_old_null_ptr) =>  {
                        // assert!(_old_null_ptr.is_null());
                        self.add_count(1, 0);
                        return None;
                    }
                    Err(changed) => {
                        // Restore node

                        // Asserting is not really necessary
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
                BinEntry::Moved => {
                    table = table.help_transfer();
                    unimplemented!();
                }
                BinEntry::Node(ref head) 
                    if no_replacement && head.hash == h && &head.key == &node.key => {
                        // Fast path if replacement is disallowed and 
                        // first bin  matches.
                        return Some(());
                } 
                BinEntry::Node(ref head) => {
                    // Bin is non-empty, need to link into it, so we must
                    // take the lock.
                    let _guard = head.lock.lock();
                    // need to check that this is still the head
                    let current_head = table.bin(bini, guard);
                    // We need to compare the pointers hence as_raw
                    if current_head.as_raw() != bin.as_raw() 
                    {
                        // nope -- try again from the start
                        continue;
                    }



                    // Yes it is still the head so we can now "own" the bin
                    // Note that there can still be readers in the bin!
                    // Own basically means there will no other writers in 
                    // this bin now.

                    // TODO: TreeBin and ReservationNode

                    // Bin count is the count of the bins you had a look at
                    let mut bin_count = 1;
                    let mut n = head;
                    let old_val = loop { 
                        if n.hash == node.hash && &n.key == node.key {
                            // the key already exists in the map
                            if no_replacement {
                                // The key is not absent, so dont update
                            } else {
                                // Value here is an atomic so we will swap 
                                // in the new value 
                                let now_garbage = n.value.swap(node.value,
                                                       Ordering::SeqCst, 
                                                       guard);
                                unimplemented!("need to dispose of garbage");
                            }
                            break Some(());
                        }
                        
                        // TODO: This ordering can probably be relaxed due to
                        // the mutex.
                        let next =  n.next.load(Ordering::SeqCst, guard);
                        if next.is_null() 
                        {
                         // We have reached the end of the bin 
                         // and now we can just put in our value
                         // Stick the node here.
                         // We have the lock so we know that nobody would 
                         // be modifying under us.
                         n.next.store(node, Ordering::SeqCst);
                         break None;
                        }

                        n = next;
                    };

                    // TODO: Treeify threshold
                    
                    // If the old_val is some, we have not added a new
                    // element so we don't need to increment the count.
                    
                    if old_val.is_none() {
                        // increment count
                        self.add_count(1, bin_count);
                    }
                    
                    return old_val;
                }
            }

        }
     }

     /// Returns the stamp bits for resizing a table of size n.
     /// Must be negative when shifted left by RESIZE_STAMP_SHIFT
     fn resize_stamp(n: isize) -> isize {
         n.leading_zeros() | (1 << (RESIZE_STAMP_BITS - 1))
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
        // For instance, in case of insert, the current is null.
        self.bins[i].compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire, guard)
    }
}
