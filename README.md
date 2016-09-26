# jumplists
A proof-of-concept implementation of a SortedMap via median-of-k jumplists.

Jumplists are a sorted singly-linked list that uses additional jump-pointer in elements
to speed up searching in the list.
This implementation is a randomized data structure that chooses the jump targets randomly.

The methods get, put and remove all have expected logarithmic time complexity,
where the expectation is w.r.t. the random structure of the jumplist.
Jumplists also offer methods to select keys by rank, which also run in expected logarithmic time.
The next method of iterators over the entries in sorted order has worst-case constant time complexity.
A jumplist can be created in linear time from a list of sorted entries.

Jumplists use three parameters:
 * t1 and t2 control how the jump targets are chosen; t1 = t2 = 0 or t1 = t2 = 1 are advisable settings.
   The larger these parameters, the more balanced the jumplist. 
   This speeds up searches, but makes updates (insertions and deletions) more costly.
 * w controls the maximal size of sublists for which no jump pointers are used; w = 100 seems advisable.
   The larger w, the less storage is used, but searches become slower.
