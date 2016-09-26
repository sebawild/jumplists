package net.wildinter.jumplists;


import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

// TODO implement NavigableMap?

/**
 * A SortedMap implementation based on median-of-k jump lists.
 * <p/>
 * null-values are allowed; the null-key is only allowed if it is properly handled by the
 * comparator
 *
 * @author Sebastian Wild (wild@cs.uni-kl.de)
 */
public class JumpListMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V> {


	private final PlainNode<K, V> tail = new PlainNode<>(null, null, null);
	private final JumpNode<K, V> head = new JumpNode<>(null, null, tail, 0, tail);
	private int size = 0;

	/**
	 * The comparator used to maintain order in this tree map, or null if it uses the natural
	 * ordering of its keys.
	 *
	 * @serial
	 */
	private final Comparator<? super K> comparator;

	private final Random random;

	private final int t1, t2, k, w;

	private static boolean REUSE_NODES_IF_POSSIBLE = true;


	/**
	 * Fields initialized to contain an instance of the entry set view the first time this
	 * view is requested.  Views are stateless, so there's no reason to create more than
	 * one.
	 */
	private transient EntrySet entrySet = null;


	/** The number of structural modifications to the tree. */
	private transient int modCount = 0;


	public JumpListMap() {
		this(naturalComparator(), new Random(), 1, 1, 5);
	}

	public JumpListMap(final int t1, final int t2, int w) {
		this(naturalComparator(), new Random(), t1, t2, w);
	}

	public JumpListMap(final Comparator<? super K> comparator, final Random random,
		  final int t1, final int t2, int w) {
		if (random == null) throw new IllegalArgumentException("null random");
		if (t1 < 0 || t2 < 0) throw new IllegalArgumentException("t1 and t2 must be >= 0");
		if (comparator != null) {
			this.comparator = comparator;
		} else {
			this.comparator = naturalComparator();
		}
		this.random = random;
		this.t1 = t1;
		this.t2 = t2;
		this.k = t1 + t2 + 1;
		if (w < k + 1) throw new IllegalArgumentException("w must be >= k+1");
		this.w = w;
	}

	public JumpListMap(SortedMap<K, V> source, final int t1, final int t2, final int w) {
		this(source, new Random(), t1, t2, w);
	}

	public JumpListMap(SortedMap<K, V> source, final Random random, final int t1,
		  final int t2, final int w) {
		this(source.comparator(), random, t1, t2, w);

		// Create plain list of sorted backbone
		Node<K, V> last = head;
		for (final Entry<K, V> entry : source.entrySet()) {
			final Node<K, V> next = new PlainNode<>(entry.getKey(), entry.getValue(), tail);
			last.setNext(next);
			last = next;
			++size;
		}

		// rebalance whole list
		switchToNewDummyHead(rebalance(head, size + 1).first);
	}

	@Override public V get(final Object key) {
		final Entry<K, V> entry = this.getEntry(key);
		return entry == null ? null : entry.getValue();
	}

	@Override public boolean containsKey(final Object key) {
		return getEntry(key) != null;
	}

	@Override public V put(final K key, final V value) {
		final Pair<Node<K, V>, Integer> lastNodeBeforeKeyAndItsRank =
			  getLastNodeBeforeAndItsRank(key);
		final Node<K, V> node = lastNodeBeforeKeyAndItsRank.first.getNext();
//		System.out.println("node = " + node);
		if (cmp(node, key) == 0) {
			return node.setValue(value);
		} else {
			addNewNode(lastNodeBeforeKeyAndItsRank, key, value);
			return null;
		}
	}

	/**
	 * Removes the mapping for this key from this JumpListMap if present.
	 *
	 * @param key key for which mapping should be removed
	 * @return the previous value associated with {@code key}, or {@code null} if there was
	 *         no mapping for {@code key}. (A {@code null} return can also indicate that the
	 *         map previously associated {@code null} with {@code key}.)
	 * @throws ClassCastException   if the specified key cannot be compared with the keys
	 *                              currently in the map
	 * @throws NullPointerException if the specified key is null and this map uses natural
	 *                              ordering, or its comparator does not permit null keys
	 */
	@Override public V remove(Object key) {
		@SuppressWarnings({"unchecked"}) K k = (K) key;
		final Pair<Node<K, V>, Integer> lastNodeBeforeKeyAndItsRank =
			  getLastNodeBeforeAndItsRank(k);
		final Node<K, V> node = lastNodeBeforeKeyAndItsRank.first.getNext();
//		System.out.println("node = " + node);
		if (cmp(node, k) == 0) {
			V oldValue = node.getValue();
			deleteNode(lastNodeBeforeKeyAndItsRank, k);
			return oldValue;
		} else {
			return null;
		}
	}

	@Override public void clear() {
		++this.modCount;
		this.size = 0;
		this.head.setNext(this.tail);
		this.head.setNextSize(0);
		this.head.setJump(this.tail);
	}

	@Override public int size() {
		return size;
	}

	@Override public Set<Entry<K, V>> entrySet() {
		EntrySet es = entrySet;
		return (es != null) ? es : (entrySet = new EntrySet());
	}

	@Override public Comparator<? super K> comparator() {
		return this.comparator;
	}

	@Override public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
		// TODO implement submap views
		throw new UnsupportedOperationException();
	}

	@Override public SortedMap<K, V> headMap(final K toKey) {
		throw new UnsupportedOperationException();
	}

	@Override public SortedMap<K, V> tailMap(final K fromKey) {
		throw new UnsupportedOperationException();
	}

	@Override public K firstKey() {
		if (size == 0) throw new NoSuchElementException();
		return this.head.getNext().getKey();
	}

	@Override public K lastKey() {
		// TODO needs a getFirstGreaterEqualNode method
		throw new UnsupportedOperationException();
	}

	@Override public void putAll(final Map<? extends K, ? extends V> m) {
		// TODO efficient bulk insert?
		super.putAll(m);
	}


	/**
	 * Spine search starting at this.head {@link #getLastNodeBefore(Object,
	 * net.wildinter.jumplists.JumpListMap.Node)}
	 */
	Node<K, V> getLastNodeBefore(K key) {
		return getLastNodeBefore(key, this.head);
	}

	/**
	 * spine search starting at dummy head
	 *
	 * @param key       key to search
	 * @param dummyHead node to start search
	 * @return the last node whose key is strictly smaller than key, or dummyHead if no such
	 *         node exists
	 */
	Node<K, V> getLastNodeBefore(K key, Node<K, V> dummyHead) {
		// Phase 1: BST-style search
		Node<K, V> head = dummyHead, lastJumped = head;
		do {
//			System.out.println("head = " + head);
			if (head.compareJumpNodeToKey(key, this.comparator) < 0) {
				head = lastJumped = head.getJump();
			} else {
				head = head.getNext();
			}
		} while (head instanceof JumpNode);
//		System.out.println("Phase 2");
		// Phase 2: linear search from last checkpoint
		head = lastJumped;
		while (cmp(head.getNext(), key) < 0) {
//			System.out.println("head = " + head);
			head = head.getNext();
		}
		return head;
	}

	/**
	 * Returns the key with (zero-based) rank r, i.e. a key in this map so that there are
	 * exactly r strictly smaller keys in this map.
	 *
	 * @param r zero-based rank, 0 <= r <= size() - 1
	 * @return key of rank r
	 */
	public K getRthKey(final int r) {
		return getNodeByRank(r).getKey();
	}

	/**
	 * Find the node with rth smallest key
	 *
	 * @param rank zero-based rank, ie, 0 <= rank <= n-1
	 * @return node with rank r
	 */
	Node<K, V> getNodeByRank(final int rank) {
		if (rank < 0 || rank >= this.size) throw new IllegalArgumentException();
		int r = rank + 1; // skip dummy head
		Node<K, V> head = this.head;
		do {
			if (r > head.getNextSize()) {
				r -= head.getNextSize() + 1;
				head = head.getJump();
			} else {
				r -= 1;
				head = head.getNext();
			}
			if (r == 0) return head;
		} while (head instanceof JumpNode);
		do {
			r -= 1;
			head = head.getNext();
		} while (r > 0);
		assert r == 0;
		return head;
	}

	/**
	 * @param key
	 * @return the last node whose key is strictly smaller than key and its zero-based rank,
	 *         i.e. the number of nodes in this map with key strictly less than key.
	 */
	Pair<Node<K, V>, Integer> getLastNodeBeforeAndItsRank(K key) {
		// spine search
		int rank = 0;
		int steppedOver = 0;
		// Phase 1: BST-style search
		Node<K, V> head = this.head, lastJumped = head;
		do {
			if (head.compareJumpNodeToKey(key, this.comparator()) < 0) {
				rank += head.getNextSize() + 1 + steppedOver;
				steppedOver = 0;
				head = lastJumped = head.getJump();
			} else {
				++steppedOver;
				head = head.getNext();
			}
		} while (head instanceof JumpNode);
		// Phase 2: linear search from last checkpoint
		head = lastJumped;
		while (cmp(head.getNext(), key) < 0) {
			++rank;
			head = head.getNext();
		}
		return Pair.of(head, rank);
	}

	/**
	 * Returns this map's entry for the given key, or {@code null} if the map does not
	 * contain an entry for the key.
	 *
	 * @param key
	 * @return this map's entry for the given key, or {@code null} if the map does not
	 *         contain an entry for the key
	 * @throws ClassCastException   if the specified key cannot be compared with the keys
	 *                              currently in the map
	 * @throws NullPointerException if the specified key is null and this map uses natural
	 *                              ordering, or its comparator does not permit null keys
	 */
	final Entry<K, V> getEntry(Object key) {
		@SuppressWarnings({"unchecked"}) K k = (K) key;
		if (key == null) throw new NullPointerException();
		final Node<K, V> candidate = getLastNodeBefore(k).getNext();
		return cmp(candidate, k) == 0 ? candidate : null;
	}


	/**
	 * Adds a new node
	 *
	 * @param lastNodeBeforeKeyAndItsRank expects this to be returned by {@code
	 *                                    getLastNodeBeforeAndItsRank(key);}
	 * @param key                         the key to insert; assumed not to be
	 * @param value                       its associated value
	 * @return the newly created node
	 * @throws IllegalStateException if key is already present in this list
	 */
	private Node<K, V> addNewNode(Pair<Node<K, V>, Integer> lastNodeBeforeKeyAndItsRank,
		  K key, V value) {
		final Node<K, V> predecessor = lastNodeBeforeKeyAndItsRank.first;
		final int rank = lastNodeBeforeKeyAndItsRank.snd;
		if (cmp(predecessor.getNext(), key) <= 0) throw new IllegalStateException(
			  "key " + key + " already present!");

		// Add the new element
		++modCount;
		++size;
		final Node<K, V> oldNext = predecessor.getNext();
		final PlainNode<K, V> newNode = new PlainNode<>(key, value, oldNext);
		predecessor.setNext(newNode);
		// +1 to account for dummy head
		switchToNewDummyHead(restoreDistributionAfterInsert(head, size + 1, rank + 1));
		return newNode;
	}

	private void switchToNewDummyHead(final Node<K, V> newHead) {
		// TODO think about thread-safety:
		// Is it sufficient to make this.head Atomic and set REUSE_NODES_IF_POSSIBLE=false?
		this.head.setNext(newHead.getNext());
		if (newHead instanceof JumpNode) {
			this.head.setJump(newHead.getJump());
			this.head.setNextSize(newHead.getNextSize());
		} else {
			this.head.setJump(this.tail);
		}
	}

	/**
	 * Flips a biased coin; returns true with probability (approximately) p.
	 *
	 * @param p the probability of a success (return value true)
	 * @return true with probability p; more precisely rand < p where rand is a pseudorandom
	 *         float between 0.0 and 1.0 (exclusive). If p=1.0f, returns true for sure; if
	 *         p=0.0f, returns true with small probability (2^-24).
	 */
	private boolean coinflip(float p) {
		if (p < 0.0f || p > 1.0f) throw new IllegalArgumentException(
			  "Need 0.0 <= p <= 1.0"); // be aggressive; more liberal choice possible
		final float rand = random.nextFloat();
//		System.out.println(indent + "COIN: " + rand + " < " + p + "?");
//		return (p == 1.0f) && rand < p; // disable randomness for debugging
		return rand < p;
	}

	private Node<K, V> deleteNode(Pair<Node<K, V>, Integer> lastNodeBeforeKeyAndItsRank,
		  K toDelete) {
		final Node<K, V> predecessor = lastNodeBeforeKeyAndItsRank.first;
		final int rank = lastNodeBeforeKeyAndItsRank.snd;
		if (cmp(predecessor.getNext(), toDelete) > 0) throw new IllegalStateException(
			  "key to delete " + toDelete + " not present!");

		// Remove the node new element
		++modCount;
		--size;
		final Node<K, V> deletedNode = predecessor.getNext();
		predecessor.setNext(deletedNode.getNext());
		// +1 to account for dummy head
		switchToNewDummyHead(restoreDistributionAfterDelete(head, size + 1, rank + 1,
			  deletedNode));
		return deletedNode;
	}


	/**
	 * Modifies jump pointers so that the same distribution as after rebalance holds after a
	 * single insertion of an element, whose rank (i.e. the number of elements strictly
	 * smaller) in the sublist is r.
	 *
	 * @param head head of sublist
	 * @param n    size of current sublist (including new element)
	 * @param r    rank of new element in this sublist
	 * @return new head of sublist (can be head, but need not be)
	 */
	private Node<K, V> restoreDistributionAfterInsert(final Node<K, V> head, final int n,
		  final int r) {
//		System.out.println(indent + "restore dist for " + head);
//		System.out.println(indent + "n=" + n + ", r=" + r);
		if (n <= w + 1) { // base case
			if (n == w + 1) { // rebalance needed, since we now need a jump pointer in head
				return rebalance(head, n).first;
			} else { // new sublist still without jumps
				return head;
			}
		} else {
			// With probability k/(n-2), new sample candidate is used.
			assert n > 2;
			final float p = ((float) k) / (n - 2);
			final boolean mustRebalanceSublist = coinflip(p);
//			System.out.println(
//				  indent + "mustRebalance " + mustRebalanceSublist + " (p=" + p + ")");
			if (mustRebalanceSublist) {
				// Rebalance, but conditional on candidate being in sample.
				int fixedIndexInSample = Math.max(2, r);
				final int jumpIndex = getRandomJumpIndexWithFixedElement(n,
					  fixedIndexInSample);
				return setJumpToJumpIndexAndRebalanceSublists(head, n, jumpIndex).first;
			} else {
				// topmost jump pointer can be kept
				final JumpNode<K, V> newHead; // the head returned at the end
				if (r == 0) { // inserted node is head --> steal jump pointer from successor
					assert head != this.head : "Flipping with dummy head";
					// successor must be JumpNode, since old n > w
					final JumpNode<K, V> successor = (JumpNode<K, V>) head.getNext();
//					System.out.println(
//						  indent + "Stealing pointer from successor " + successor);
					newHead = stealPointersFromSuccessor(head, successor);
				} else {
					// sublist can keep jump, so keep head
					newHead = (JumpNode<K, V>) head;
				}
//				System.out.println(indent + "newHead = " + newHead);
//				System.out.println(indent + "r <= newHead.getNextSize() + 1 ? " + (r
//					  <= newHead.getNextSize() + 1));
				if (r <= newHead.getNextSize() + 1) { // new element in next list
					newHead.incrementNextSize();
					final int newR = Math.max(0, r - 1); // one smaller, except for r=0
					final int newN = newHead.getNextSize();
//					String oldIndent = indent;
//					indent += "\t";
					final Node<K, V> newSucc = restoreDistributionAfterInsert(
						  newHead.getNext(), newN, newR);
//					indent = oldIndent;
					newHead.setNext(newSucc);
				} else {
					// new element in jump list
					final int newN = n - 1 - newHead.getNextSize();
					final int newR = r - 1 - newHead.getNextSize();
//					String oldIndent = indent;
//					indent += "\t";
					final Node<K, V> newJump = restoreDistributionAfterInsert(
						  newHead.getJump(), newN, newR);
//					indent = oldIndent;
					if (newJump != newHead.getJump()) { // Jump list got rebalanced
//						System.out.println(indent + "Jump target changed!");
						newHead.setJump(newJump);
						// Reconnect nextlist to new jump
						getLastNodeBefore(newJump.getKey(), newHead).setNext(newJump);
					}
				}
				return newHead;
			}
		}
	}

	private JumpNode<K, V> stealPointersFromSuccessor(final Node<K, V> head,
		  final JumpNode<K, V> successor) {
		if (REUSE_NODES_IF_POSSIBLE) {
			K succKey = successor.getKey();
			V succValue = successor.getValue();
			successor.setKey(head.getKey());
			successor.setValue(head.getValue());
			head.setKey(succKey);
			head.setValue(succValue);
			head.setNext(successor.getNext());
			successor.setNext(head);
			return successor;
		} else {
			return new JumpNode<>(head.getKey(), head.getValue(), new PlainNode<>(
				  successor.getKey(), successor.value, successor.getNext()),
				  successor.getNextSize(), successor.getJump());
		}
	}

	/**
	 * Relocated jump pointers after the rth element has been deleted to have the same
	 * distribution as after rebalance.
	 *
	 * @param head        head of the sublist
	 * @param n           size of the current sublist (without the deleted element)
	 * @param r           zero-based rank of the deleted element, i.e. there are r nodes with
	 *                    strictly smaller key in this JumpListMap.
	 * @param deletedNode the deleted node (already removed from this sublist)
	 * @return new head of this sublist (might be head, but can change due to rebalancing)
	 */
	private Node<K, V> restoreDistributionAfterDelete(final Node<K, V> head, final int n,
		  final int r, Node<K, V> deletedNode) {
//		System.out.println(indent + "restore dist for " + head);
//		System.out.println(indent + "n=" + n + ", r=" + r);
//		System.out.println(indent + "deletedNode = " + deletedNode);
		if (n <= w) { // base case
			if (n == w && head instanceof JumpNode) {
				// head must become a PlainNode
				return new PlainNode<>(head.getKey(), head.getValue(), head.getNext());
			} else { // no change needed
				return head;
			}
		} else {
			final int nextSize = (r == 0) ? deletedNode.getNextSize() : head.getNextSize();
			final boolean mustRebalanceSublist;
			if (r == nextSize + 1) { // deleted jump target
				assert head.getJump() == deletedNode;
				mustRebalanceSublist = true;
//				System.out.println(indent + "mustRebalance " + mustRebalanceSublist
//					  + " (deleted jump target)");
			} else if (k == 1) { // handle as special case to avoid p = 0/0 cases.
				// Only have to rebalance if jump pointer invalid after deletion.
				mustRebalanceSublist = (nextSize == 1) && r <= 1;
//				System.out.println(indent + "mustRebalanceSublist = " + mustRebalanceSublist
//					  + " (k=1; jump now invalid?)");
			} else {
				final float p;
				if (r < nextSize + 1) {
					p = ((float) t1) / (nextSize - 1);
				} else {
					p = ((float) t2) / ((n + 1) - 2 - nextSize);
				}
				/* Note on corner cases:
				 *   (a) t == denominator
				 *       p = 1.0 and we rebalance for sure.
				 */
				mustRebalanceSublist = coinflip(p);
//				System.out.println(
//					  indent + "mustRebalance " + mustRebalanceSublist + " (p=" + p + ")");
			}
			if (mustRebalanceSublist) {
				return rebalance(head, n).first;
			} else {
				// topmost jump pointer can be kept
				final JumpNode<K, V> newHead; // the head returned at the end
				if (r == 0) {
					// Impose old predecessor deletedNode's jump on head.
					// deleted node must be JumpNodes, since n > w
//					System.out.println(indent + "Imposing deleted pointer on head");
//					System.out.println(indent + "head = " + head);
//					System.out.println(indent + "deletedNode = " + deletedNode);
					final JumpNode<K, V> jDeletedNode = (JumpNode<K, V>) deletedNode;
					newHead = imposePointerOnSuccessor(head, jDeletedNode);
					deletedNode = head;
				} else {
					// head keeps its jump
					newHead = (JumpNode<K, V>) head;
				}
//				System.out.println(indent + "newHead = " + newHead);
//				System.out.println(indent + "r < nextSize + 1 ? " + (r < nextSize + 1));
				if (r < nextSize + 1) { // deletion in next list
					newHead.decrementNextSize();
					final int newR = Math.max(0, r - 1); // one smaller, except for r=0
					final int newN = newHead.getNextSize();
//					String oldIndent = indent;
//					indent += "\t";
					final Node<K, V> newSucc = restoreDistributionAfterDelete(
						  newHead.getNext(), newN, newR, deletedNode);
//					indent = oldIndent;
					newHead.setNext(newSucc);
				} else {
					// new element in jump list
					final int newN = n - 1 - newHead.getNextSize();
					final int newR = r - 1 - newHead.getNextSize();
//					String oldIndent = indent;
//					indent += "\t";
					final Node<K, V> newJump = restoreDistributionAfterDelete(
						  newHead.getJump(), newN, newR, deletedNode);
//					indent = oldIndent;
					if (newJump != newHead.getJump()) { // Jump list got rebalanced
//						System.out.println(indent + "Jump target changed!");
						newHead.setJump(newJump);
						// Reconnect nextlist to new jump
						getLastNodeBefore(newJump.getKey(), newHead).setNext(newJump);
					}
				}
				return newHead;
			}
		}
	}

	private JumpNode<K, V> imposePointerOnSuccessor(final Node<K, V> head,
		  final JumpNode<K, V> deletedNode) {
		if (REUSE_NODES_IF_POSSIBLE) {
			// Flip nodes
			deletedNode.setKey(head.getKey());
			deletedNode.setValue(head.getValue());
			deletedNode.setNext(head.getNext());
			// keep jump and nextsize
			return deletedNode;
		} else {
			return new JumpNode<>(head.getKey(), head.getValue(), head.getNext(),
				  deletedNode.getNextSize(), deletedNode.getJump());
		}
	}

	static String indent = "";

	/**
	 * rebalance sublist starting with head (inclusive) and continuing for sublistSize nodes
	 * (inclusive).
	 *
	 * @param head        head of sublist
	 * @param sublistSize number of nodes in sublist including head; this must already
	 *                    reflect any recent insertion or deletion
	 * @return start and end of the new list (nodes might change)
	 */
	private Pair<Node<K, V>, Node<K, V>> rebalance(final Node<K, V> head,
		  final int sublistSize) {
//		System.out.println(indent + "head = " + head);
//		System.out.println(indent + "sublistSize = " + sublistSize);
		++modCount;
		if (sublistSize <= 0) throw new AssertionError(
			  "should never call rebalance on empty list");
		if (sublistSize <= w) {
			// Replace sublist by chain of plain nodes
			Node<K, V> newHead = null, last = newHead, old = head;
			for (int i = 0; i < sublistSize; ++i) {
//				System.out.println(indent + old);
				final PlainNode<K, V> newNode =
					  REUSE_NODES_IF_POSSIBLE && old instanceof PlainNode
							 ? (PlainNode<K, V>) old : new PlainNode<>(old.getKey(),
							 old.getValue(), old.getNext());
				old = old.getNext();
				if (newHead == null) {
					newHead = newNode; // store first new
				} else {
					last.setNext(newNode); // or link to in predecessor
				}
				last = newNode;
			}
//			System.out.println(indent + "last = " + last);
			return Pair.of(newHead, last);
		} else {
			final int jumpIndex = getRandomJumpIndex(sublistSize);
//			System.out.println(indent + "jumpIndex = " + jumpIndex);
//			String oldIndent = indent;
//			indent = indent + "\t";
			return setJumpToJumpIndexAndRebalanceSublists(head, sublistSize, jumpIndex);
		}
	}

	/**
	 * Like {@link #rebalance(net.wildinter.jumplists.JumpListMap.Node, int)}, but enforces
	 * the given jumpIndex at the top level.
	 *
	 * @param head
	 * @param sublistSize
	 * @param jumpIndex
	 * @return
	 */
	private Pair<Node<K, V>, Node<K, V>> setJumpToJumpIndexAndRebalanceSublists(
		  final Node<K, V> head, final int sublistSize, final int jumpIndex) {
		final Pair<Node<K, V>, Node<K, V>> nextList = rebalance(head.getNext(),
			  jumpIndex - 1);
//			System.out.println(oldIndent + "---");
		final Pair<Node<K, V>, Node<K, V>> jumpList = rebalance(nextList.snd.getNext(),
			  sublistSize - jumpIndex);
		// reconnect lists
		nextList.snd.setNext(jumpList.first);
//			indent = oldIndent;
		JumpNode<K, V> newHead =
			  REUSE_NODES_IF_POSSIBLE && head instanceof JumpNode ? (JumpNode<K, V>) head
					 : new JumpNode<>(head.getKey(), head.getValue());
		newHead.setNext(nextList.first);
		newHead.setJump(jumpList.first);
		newHead.setNextSize(jumpIndex - 1);
//			System.out.println(indent + "newHead = " + newHead);
		return Pair.of((Node<K, V>) newHead, jumpList.snd);
	}

	private SortedMap<K, K> getJumpEdges(K headVal) {
		final TreeMap<K, K> res = new TreeMap<>(this.comparator());
		final NodeIterator iter = new NodeIterator();
		while (iter.hasNext()) {
			final Node<K, V> node = iter.next();
			if (node instanceof JumpNode) {
				JumpNode<K, V> jumpNode = (JumpNode<K, V>) node;
				res.put(jumpNode.getKey(), jumpNode.getJump().getKey());
			}
		}
		if (size > w) res.put(headVal, this.head.getJump().getKey());
		return res;
	}


	/**
	 * @param n length of sublist
	 * @return a randomly chosen (zero-based) jump rank, i.e. the number of elements in this
	 *         sublist whose keys should be strictly smaller than the jump target's.
	 */
	private int getRandomJumpIndex(int n) {
		if (k == 1) return 2 + random.nextInt(n - 2);
		int[] sample = new int[k];
		for (int i = 0; i < k; ++i) {
			sample[i] = 2 + random.nextInt(n - 2 - i);
			for (int j = 0; j < i; ++j) {
				sample[i] += sample[j] <= sample[i] ? 1 : 0;
			}
			insertElementIntoSortedPrefix(sample, i);
		}
		return sample[t1];
	}

	private int getRandomJumpIndexWithFixedElement(int n, int fixedRank) {
		if (fixedRank < 2 || fixedRank > n - 1) throw new IllegalArgumentException(
			  "(n=" + n + ",fixedRank=" + fixedRank + ")");
		if (k == 1) return fixedRank;
		int[] sample = new int[k];
		sample[0] = fixedRank;
		for (int i = 1; i < k; ++i) {
			sample[i] = 2 + random.nextInt(n - 2 - i);
			for (int j = 0; j < i; ++j) {
				sample[i] += sample[j] <= sample[i] ? 1 : 0;
			}
			insertElementIntoSortedPrefix(sample, i);
		}
		return sample[t1];
	}

	/**
	 * Sorts {@code A[0..i]} ascendingly under the assumption that {@code A[0..i-1]} is
	 * already sorted ascendingly. This is effectively one iteration of Insertionsort.
	 *
	 * @param A the array
	 * @param i the index in A of the new element
	 */
	private static void insertElementIntoSortedPrefix(int[] A, int i) {
		final int v = A[i];
		int j = i - 1;
		while (j >= 0 && v < A[j]) {
			A[j + 1] = A[j];
			--j;
		}
		A[j + 1] = v;
	}

	private static <K> Comparator<K> naturalComparator() {
		return new Comparator<K>() {
			@SuppressWarnings({"unchecked"}) @Override
			public final int compare(final K o1, final K o2) {
				return ((Comparable<K>) o1).compareTo(o2);
			}
		};
	}


	private int cmp(Node<K, V> node, K key) {
//		System.out.println("cmp: " + node.getKey() + " vs " + key);
		// avoid explicit check on tail
		try {
			return comparator.compare(node.getKey(), key);
		} catch (NullPointerException e) {
			return 1;
		}
	}

	private abstract static class Node<K, V> implements Map.Entry<K, V> {
		protected K key;
		protected V value;
		protected Node<K, V> next;


		protected Node(K key, V value, final Node<K, V> next) {
			this.key = key;
			this.value = value;
			this.next = next;
		}

		protected Node(final K key, final V value) {
			this.key = key;
			this.value = value;
		}

		protected Node() {
		}

		public Node<K, V> getNext() {
			return next;
		}

		public void setNext(final Node<K, V> next) {
			this.next = next;
		}

		public K getKey() {
			return key;
		}

		public void setKey(final K key) {
			this.key = key;
		}

		public V getValue() {
			return value;
		}

		@Override public V setValue(final V value) {
			final V old = this.value;
			this.value = value;
			return old;
		}

		@Override public String toString() {
			return "Entry(" + "key=" + key + ", value=" + value + ')';
		}

		abstract int compareJumpNodeToKey(K other, final Comparator<? super K> comparator);

		public abstract Node<K, V> getJump();

		public abstract int getNextSize();
	}

	private static final class PlainNode<K, V> extends Node<K, V> {
		private PlainNode(final K key, final V value, final Node<K, V> next) {
			super(key, value, next);
		}

		@Override public String toString() {
			return "Plain(" + key + ": " + value + ')';
		}

		/**
		 * always claims jump node is larger
		 *
		 * @param other
		 * @param comparator
		 * @return
		 */
		@Override
		int compareJumpNodeToKey(final K other, final Comparator<? super K> comparator) {
			// a non-existant jump pointer is always 'too large'
			return 1;
		}

		@Override public Node<K, V> getJump() {
			throw new UnsupportedOperationException();
		}

		@Override public int getNextSize() {
			throw new UnsupportedOperationException();
		}
	}

	private static final class JumpNode<K, V> extends Node<K, V> {
		private int nextSize;
		private Node<K, V> jump;

		private JumpNode(final K key, final V value, final Node<K, V> next,
			  final int nextSize, final Node<K, V> jump) {
			super(key, value, next);
			this.nextSize = nextSize;
			this.jump = jump;
		}

		private JumpNode(final K key, final V value) {
			super(key, value);
		}

		private JumpNode() {
		}

		public int getNextSize() {
			return nextSize;
		}

		public void setNextSize(final int nextSize) {
			this.nextSize = nextSize;
		}

		public void incrementNextSize() {
			this.setNextSize(this.getNextSize() + 1);
		}

		public void decrementNextSize() {
			this.setNextSize(this.getNextSize() - 1);
		}

		@Override public Node<K, V> getJump() {
			return jump;
		}

		public void setJump(final Node<K, V> jump) {
			this.jump = jump;
		}

		@Override
		int compareJumpNodeToKey(final K other, final Comparator<? super K> comparator) {
//			System.out.println("cmp: " + this.getJump().getKey() + " vs " + other);
			try {
				return comparator.compare(this.getJump().getKey(), other);
			} catch (NullPointerException e) {
				return 1;
			}
		}

		@Override public String toString() {
			return "Jump(" + key + ": " + value + ", ns=" + nextSize + ", j=" + jump.getKey()
				  + ')';
		}
	}

	public static final class Pair<T1, T2> {
		final T1 first;
		final T2 snd;

		private Pair(final T1 first, final T2 snd) {
			this.first = first;
			this.snd = snd;
		}

		@Override public String toString() {
			return "(" + first + ", " + snd + ')';
		}

		public static <T1, T2> Pair<T1, T2> of(T1 t1, T2 t2) {
			return new Pair<>(t1, t2);
		}
	}

	/**
	 * Test two values for equality.  Differs from o1.equals(o2) only in that it copes with
	 * {@code null} o1 properly.
	 *
	 * @param o1
	 * @param o2
	 * @return
	 */
	static boolean valEquals(Object o1, Object o2) {
		return (o1 == null ? o2 == null : o1.equals(o2));
	}

	abstract class PrivateEntryIterator<T> implements Iterator<T> {
		Node<K, V> next;
		Node<K, V> lastReturned;
		int expectedModCount;

		PrivateEntryIterator() {
			this(JumpListMap.this.head.getNext());
		}

		PrivateEntryIterator(Node<K, V> first) {
			expectedModCount = modCount;
			lastReturned = null;
			next = first;
		}

		public final boolean hasNext() {
			return next != JumpListMap.this.tail;
		}

		final Node<K, V> nextEntry() {
			Node<K, V> e = next;
			if (e == null) throw new NoSuchElementException();
			if (modCount != expectedModCount) throw new ConcurrentModificationException();
			next = next.getNext();
			lastReturned = e;
			return e;
		}

		public void remove() {
			if (lastReturned == null) {
				throw new IllegalStateException();
			}
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}

			final K key = lastReturned.getKey();
			JumpListMap.this.remove(key);
			this.next = getLastNodeBefore(key).getNext();
			// Can we do better than deleting globally and
			// then finding the position to continue again?

			expectedModCount = modCount;
			lastReturned = null;
		}
	}

	final class EntryIterator extends PrivateEntryIterator<Map.Entry<K, V>> {
		EntryIterator() {
		}

		EntryIterator(Node<K, V> first) {
			super(first);
		}

		public Map.Entry<K, V> next() {
			return nextEntry();
		}
	}

	final class NodeIterator extends PrivateEntryIterator<Node<K, V>> {
		NodeIterator() {
		}

		NodeIterator(final Node<K, V> first) {
			super(first);
		}

		@Override public Node<K, V> next() {
			return nextEntry();
		}
	}


	class EntrySet extends AbstractSet<Entry<K, V>> {

		public Iterator<Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry)) return false;
			@SuppressWarnings({"unchecked"}) Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
			V value = entry.getValue();
			Entry<K, V> p = getEntry(entry.getKey());
			return p != null && valEquals(p.getValue(), value);
		}

		public boolean remove(Object o) {
			if (!(o instanceof Map.Entry)) return false;
			@SuppressWarnings({"unchecked"}) Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
			V value = entry.getValue();
			Entry<K, V> p = getEntry(entry.getKey());
			if (p != null && valEquals(p.getValue(), value)) {
				JumpListMap.this.remove(p);
				return true;
			}
			return false;
		}

		public int size() {
			return JumpListMap.this.size();
		}

		public void clear() {
			JumpListMap.this.clear();
		}
	}


	/*
	 * Tests
	 */

	private enum ExperimentDataStructures {
		JUMPLIST {
			@Override SortedMap<Integer, Integer> createInstance(final int t1, final int t2,
				  final int w, final int n, final Random jlRand,
				  final SortedMap<Integer, Integer> source) {
				return source != null ? new JumpListMap<>(source, jlRand, t1, t2, w)
					  : new JumpListMap<>(new TreeMap<Integer, Integer>(), jlRand, t1, t2, w);
			}
		},
		TREE_MAP {
			@Override SortedMap<Integer, Integer> createInstance(final int t1, final int t2,
				  final int w, final int n, final Random jlRand,
				  final SortedMap<Integer, Integer> source) {
				return source != null ? new TreeMap<>(source)
					  : new TreeMap<Integer, Integer>();
			}
		};

		abstract SortedMap<Integer, Integer> createInstance(final int t1, final int t2,
			  final int w, final int n, final Random jlRand,
			  SortedMap<Integer, Integer> source);
	}

	private enum RunningTimeExperiments {
		CREATE_FROM_SORTED_SOURCE {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  ExperimentDataStructures ds) {
				final TreeMap<Integer, Integer> source = createSourceMap(n, false);
				int dummy = 0;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					final SortedMap<Integer, Integer> instance = ds.createInstance(t1, t2, w,
						  n, jlRand, source);
					dummy += instance.size();
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		RANDOM_INSERTS_INTO_EMPTY_LIST {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
						  jlRand, null);
					for (int i = 0; i < n; ++i) {
						final int x = inputRand.nextInt();
						map.put(x, x >> 16);
					}
					dummy += map.size();
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		SEARCH_EACH_ELEMENT_CREATED_FROM_SOURCE {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, createSourceMap(n, false));
				final int twoN = 2 * n;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (int i = 2; i <= twoN; i += 2) {
						final int value = map.get(i);
						dummy += value;
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		SEARCH_EACH_ELEMENT_RANDOMLY_GENERATED {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, null);
				insertKeysFromSourceInRandomOrder(n, map, inputRand);
				final int twoN = 2 * n;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (int i = 2; i <= twoN; i += 2) {
						final int value = map.get(i);
						dummy += value;
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		SEARCH_EACH_GAP_CREATED_FROM_SOURCE {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, createSourceMap(n, false));
				final int twoNPlusOne = 2 * n + 1;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (int i = 1; i <= twoNPlusOne; i += 2) {
						final int value = map.get(i);
						dummy += value;
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		SEARCH_EACH_GAP_RANDOMLY_GENERATED {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, null);
				insertKeysFromSourceInRandomOrder(n, map, inputRand);
				final int twoNPlusOne = 2 * n + 1;

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (int i = 1; i <= twoNPlusOne; i += 2) {
						final int value = map.get(i);
						dummy += value;
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		RANDOM_INSERT_AND_DELETE {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, null);
				insertKeysFromSourceInRandomOrder(n, map, inputRand);

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					final int x = 2 * inputRand.nextInt(n) + 1;
					map.put(x, x >> 1);
					dummy += map.size();
					map.remove(x);
				}
				final long end = System.currentTimeMillis();
				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;
			}
		},
		TRAVERSE_RANDOMLY_GENERATED {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, null);
				insertKeysFromSourceInRandomOrder(n, map, inputRand);

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (final Entry<Integer, Integer> entry : map.entrySet()) {
						dummy += entry.getKey();
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;

			}
		},
		TRAVERSE_GENERATED_FROM_SOURCE {
			@Override long doExperiment(final int t1, final int t2, final int w, final int n,
				  final int repetitions, final Random jlRand, final Random inputRand,
				  final ExperimentDataStructures ds) {
				int dummy = 0;
				final SortedMap<Integer, Integer> map = ds.createInstance(t1, t2, w, n,
					  jlRand, createSourceMap(n, false));

				final long start = System.currentTimeMillis();
				for (int runs = 0; runs < repetitions; ++runs) {
					for (final Entry<Integer, Integer> entry : map.entrySet()) {
						dummy += entry.getKey();
					}
				}
				final long end = System.currentTimeMillis();

				System.err.println("\t\t" + dummy); // do something with the instance
				return end - start;

			}
		};

		static TreeMap<Integer, Integer> createSourceMap(final int n,
			  final boolean withComparator) {
			TreeMap<Integer, Integer> source =
				  withComparator ? new TreeMap<Integer, Integer>(new Comparator<Integer>() {
					  @Override public int compare(final Integer o1, final Integer o2) {
						  return o1 - o2;
					  }
				  }) : new TreeMap<Integer, Integer>();
			for (int i = 1; i <= n; ++i) {
				source.put(2 * i, 2 * i - 1);
			}
			return source;
		}

		private static void insertKeysFromSourceInRandomOrder(final int n,
			  final SortedMap<Integer, Integer> map, final Random inputRand) {
			final TreeMap<Integer, Integer> source = createSourceMap(n, false);
			final ArrayList<Integer> keys = new ArrayList<>(n);
			for (final Integer key : source.keySet()) {
				keys.add(key);
			}
			Collections.shuffle(keys, inputRand);
			for (final Integer key : keys) {
				map.put(key, key >> 16);
			}
		}

		abstract long doExperiment(final int t1, final int t2, final int w, final int n,
			  final int repetitions, final Random jlRand, final Random inputRand,
			  final ExperimentDataStructures ds);
	}


	public static void main(String[] args) {

//		testRandomJumpIndex();
//		tryoutsWithMathematicaExport();

		if (args.length < 7) {
			printUsage();
		}

		System.err.println("\n");
		System.err.println("> JumpListMain ");
		for (final String arg : args) {
			System.err.print(arg + " ");
		}
		System.err.println("\n");

		final int t1 = Integer.parseInt(args[0]);
		final int t2 = Integer.parseInt(args[1]);
		final int w = Integer.parseInt(args[2]);
		final int n = Integer.parseInt(args[3]);
		RunningTimeExperiments experiment = null;
		try {
			experiment = RunningTimeExperiments.valueOf(args[4]);
		} catch (IllegalArgumentException e) {
			// ignore and try to fill by index
			try {
				experiment = RunningTimeExperiments.values()[Integer.parseInt(args[4])];
			} catch (NumberFormatException ex) {
				printUsage();
			}
		}
		final int repetitions = Integer.parseInt(args[5]);
		final long seed = Long.parseLong(args[6]);


		final Random jlRand = new Random(seed);
		final Random inputRand = new Random(seed);


		// Warmup
		System.err.println("Doing warmup");
		TreeMap<Integer, Integer> source = RunningTimeExperiments.createSourceMap(n, true);
		TreeMap<Integer, Integer> treeMap = new TreeMap<>(source);
		JumpListMap<Integer, Integer> jl = new JumpListMap<>(source, jlRand, t1, t2, w);
		long sum = 0;
		for (int i = 0; i < 1200; ++i) {
			final int key = inputRand.nextInt(2 * n);
			treeMap.put(key, 42);
			jl.put(key, 42);
			sum += treeMap.get(key);
			sum += jl.get(key);
			treeMap.remove(key);
			jl.remove(key);
		}

		System.err.println("ds\tt1\tt2\tw\tn\texperiment\trepetitions\tseed\tresult");

		Map<Integer, Integer> map;
		long start, end;

		for (ExperimentDataStructures ds : ExperimentDataStructures.values()) {
			System.err.println("Running " + experiment + " for " + ds);
			final long totalTime = experiment.doExperiment(t1, t2, w, n, repetitions, jlRand,
				  inputRand, ds);
			final double avgMs = ((double) totalTime) / repetitions;
			System.out.println(
				  ds + "\t" + t1 + "\t" + t2 + "\t" + w + "\t" + n + "\t" + experiment + "\t"
						 + repetitions + "\t" + seed + "\t" + avgMs);
		}


	}

	private static void printUsage() {
		System.err.println("Usage: main t1 t2 w n experiment repetitions seed");
		System.err.println("where 'experiment' is one of " + Arrays.toString(
			  RunningTimeExperiments.values()));
		System.exit(1);
	}

	private static void tryoutsWithMathematicaExport() {
		TreeMap<Integer, Integer> source = RunningTimeExperiments.createSourceMap(30,
			  false);

		final Random rand = new Random(83265235);
		JumpListMap<Integer, Integer> jl = new JumpListMap<>(source, rand, 0, 0, 2);
//		jl = new JumpListMap<>(source, rand, 1, 1, 4);
		rand.setSeed(System.currentTimeMillis());

		System.out.println("jl.size() = " + jl.size());

		printMathematicaListsForJumpList(jl);

		jl.put(37, 42);
		System.out.println("jl = " + jl);
		System.out.println("jl.size() = " + jl.size());
		printMathematicaListsForJumpList(jl);
		jl.put(19, 42);
		System.out.println("jl = " + jl);
		System.out.println("jl.size() = " + jl.size());

		jl.remove(40);

		System.out.println("jl.getRthKey(0) = " + jl.getRthKey(0));
		System.out.println("jl.getRthKey(10) = " + jl.getRthKey(10));

		printMathematicaListsForJumpList(jl);
	}

	private static void printMathematicaListsForJumpList(
		  final JumpListMap<Integer, Integer> jl) {
		System.out.print("\n\n\n{-1");
		for (final Entry<Integer, Integer> entry : jl.entrySet()) {
			System.out.print("," + entry.getKey());
		}
		System.out.println("}");

		System.out.print("{");
		for (final Entry<Integer, Integer> edge : jl.getJumpEdges(-1).entrySet()) {
			System.out.print("{" + edge.getKey() + "," + edge.getValue() + "},");
		}
		System.out.println("Nothing}\n\n\n");
	}


	private static void testRandomJumpIndex() {
		JumpListMap<Integer, Integer> jl = new JumpListMap<>(2, 2, 6);

		int n = 10;
		// Test randomJumpIndex
		long start = System.currentTimeMillis();
		int reps = 1000000;
		Map<Integer, Integer> counts = new TreeMap<>();
		for (int i = 0; i < reps; ++i) {
			final int p = jl.getRandomJumpIndex(n);
			final Integer old = counts.get(p);
			counts.put(p, old == null ? 1 : old + 1);
		}
		long end = System.currentTimeMillis();
		System.out.println("counts = " + counts);
		System.out.println("end-start = " + (end - start));

		int fixedRank = n / 2;
		start = System.currentTimeMillis();
		counts = new TreeMap<>();
		for (int i = 0; i < reps; ++i) {
			final int p = jl.getRandomJumpIndexWithFixedElement(n, fixedRank);
			final Integer old = counts.get(p);
			counts.put(p, old == null ? 1 : old + 1);
		}
		end = System.currentTimeMillis();
		System.out.println("counts = " + counts);
		System.out.println("end-start = " + (end - start));
	}
}
