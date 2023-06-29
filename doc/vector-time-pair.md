# Vector Time Pair
## Definition
Vector time pair consists of the vector modification time and the vector synchronization time, where the unit in the vector is represented as __machine to time__. For modification time, the unit is represented as __machine to modification time__, and for synchronization time, the unit is represented as __machine to synchronization time__. These vector time pairs are used to determine the conflicts happened in the synchronization process.

For directory, the vector synchronization time of it is the element-wise minimum of the synchronization times of its children, and the vector modification time of it is the element-wise maximum of the modification times of its children. 

## Sync Algorithm
The sync algorithm is based on the vector time pair. Suppose a file is existed on both server A and server B. The algorithm is described as follows:

```
sync(A -> B, F, m, s) {
  if m(A) <= s(B) {
    // F(B) is the same as or derived from F(A) 
    do nothing
  } else if m(B) <= s(A) {
    // F(A) is derived from F(B) 
    copy F(A) to F(B)
  } else {
    // neither file is derived from the other
    report a conflict
  }
}
```

Suppose a file is existed on server A, but not on server B. The algorithm is described as follows:

```
sync(A -> B, F, m, s) {
  if m(A) <= s(B) {
    // the deleted F(B) was derived from F(A)
    do nothing
  } else if c(A) > s(B) {
    // F(A) and F(B) were created independently
    // sync(B -> A, F) is meaningless
    copy F(A) to F(B)
  } else
}
```

Where the creation time is the first element in the file's modification history.

If we wanna synchronize the directory, the algorithm is described as follows: 

```
sync(A -> B, D, m, s) {
  if m(A) <= s(B) {
    // D(B) has all the changes present in A's tree
    do nothing
  } else {
    // recurse into the tree
    for child in D(A) {
      sync(A -> B, D', m', s')
    }
  }
}
```