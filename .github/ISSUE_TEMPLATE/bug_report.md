---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

<!--
  Make sure you have read CONTRIBUTING.md completely before you file a new
  issue! 
-->

## Versions/Environment
1. What version of Rust are you using? 
2. What operating system are you using?
3. What versions of the driver and its dependencies are you using? (Run 
`cargo pkgid mongodb` & `cargo pkgid bson`)
4. What version of MongoDB are you using? (Check with the MongoDB shell using `db.version()`) 
5. What is your MongoDB topology (standalone, replica set, sharded cluster, serverless)?



## Describe the bug
A clear and concise description of what the bug is.

**BE SPECIFIC**:
* What is the _expected_ behavior and what is _actually_ happening?
* Do you have any particular output that demonstrates this problem?
* Do you have any ideas on _why_ this may be happening that could give us a
clue in the right direction?
* Did this issue arise out of nowhere, or after an update (of the driver,
server, and/or Rust)? 
* Are there multiple ways of triggering this bug (perhaps more than one
function produce a crash)?
* If you know how to reproduce this bug, please include a code snippet here:
```

```


**To Reproduce**
Steps to reproduce the behavior:
1. First, do this.
2. Then do this.
3. After doing that, do this.
4. And then, finally, do this.
5. Bug occurs.
