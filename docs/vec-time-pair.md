# Vector Time Pair
## Definition
Vector time pair consists of the vector modification time and the vector synchronization time, where the unit in the vector is represented as __machine to time__. For modification time, the unit is represented as __machine to modification time__, and for synchronization time, the unit is represented as __machine to synchronization time__. These vector time pairs are used to determine the conflicts happened in the synchronization process.

For directory, the vector synchronization time of it is the element-wise minimum of the synchronization times of its children, and the vector modification time of it is the element-wise maximum of the modification times of its children. 

## Comparison
We could regard modification time vector as an outline of the file history. Therefore, the comparison of file history is done by the comparison of modification time vector. The comparison details are listed as follows:
- If the vectors are identical, so are the histories. 
- If all elements in **u** are less than or equal to the corresponding elements in **v**, then the history represented by **u** is a prefix of the history represented by **v**.
- If all elements in **v** are less than or equal to the corresponding elements in **u**, then the history represented by **v** is a prefix of the history represented by **u**.
- Otherwise, neither history is a prefix of the other, then the conflict happens.

## Sync Algorithm

In the following sections, "L With R" means that it's L in the local server and it's R on the remote server.

### File With File Sync

#### Existing File With Existing File Sync

```
sync(B <- A, F, m, s) {
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

#### Non-Existing File With Existing File Sync

```
sync(B <- A, F, m, s) {
  if m(A) <= s(B) {
    // the deleted F(B) was derived from F(A)
    do nothing
  } else if c(A) !<= s(B) {
    // F(A) and F(B) were created independently
    // sync(B -> A, F) is meaningless
    copy F(A) to F(B)
  } else {
    // they diverge, and to keep it or not cannot be determined
    report a conflict
  }
}
```

Where the creation time is the first element in the file's modification history.

#### Existing File With Non-Existing File Sync

```
sync(B <- A, F, m, s) {
  if m(A) <= s(B) {
    // the F(B) was created after F(A) was deleted
    do nothing
  } else if m(B) <= s(A) {
    delete F(B)
  } else {
    // they diverge, and to keep it or not cannot be determined
    report a conflict
  }
}
```

### Dir With Dir Sync

#### Existing Dir With Existing Dir Sync

```
sync(B <- A, D, m, s) {
  if m(A) <= s(B) {
    // D(B) has all the changes present in A's tree
    do nothing
  } else {
    // recurse into the tree
    for child in D(A) union D(B) {
      sync(B <- A, D'/F', m', s')
    }
  }
}
```

#### Non-Existing Dir With Existing Dir Sync

```
sync(B <- A, D, m, S) {
  if m(A) <= s(B) {
    // the deleted D(B) was derived from D(A)
    do nothing
  } else {
    // cannot be determined, check each file in the subtree independently
    for child in D(A) union D(B) {
      sync(B <- A, D'/F', m', s')
    }
  }
}
```

#### Existing Dir With Non-Existing Dir Sync

```
sync(B <- A, D, m, s) {
  if m(A) <= s(B) {
    // the D(B) was created after D(A) was deleted
    // cannot use s(A) here, because it's not the maximum of the subtree
    do nothing
  } else {
    // cannot be determined, check each file in the subtree independently
    for child in D(A) union D(B) {
      sync(B <- A, D'/F', m', s')
    }
  }
}
```

### File With Dir Sync

#### Existing File With Existing Dir Sync

```
sync(B <- A, D/F, m, s) {
  if m(A) <= s(B) {
    // ignore
    do nothing
  } else if m(B) <= s(A) {
    // D(A) is derived from F(B)
    copy D(A) to F(B) which would become D(B)
  } else {
    // neither file is derived from the other
    report a conflict
  }
}
```

#### Non-Existing File With Existing Dir Sync

Meaningless.

#### Existing File With Non-Existing Dir Sync

Meaningless.

#### Conclusion

In the analysis above, we can see that the sync algorithm is almost the same with file-file sync algorithm except the copy procedure might be more difficult.

### Dir With File Sync

#### Existing Dir With Existing File Sync

```
sync(B <- A, D/F, m, s) {
  if m(A) <= s(B) {
    // D(B) has all the changes present in A's tree
    do nothing
  } else if m(B) <= s(A) {
    // F(A) is derived from D(B)
    copy F(A) to D(B) which would become F(B)
  } else {
    report a conflict
  }
}
```

#### Conclusion

The other two cases are also meaningless due to the previous analysis. Besides, the sync algorithm is also like the file-file sync algorithm.