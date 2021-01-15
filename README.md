# CornucopiaDB

_:shell: The "horn of plenty" – a symbol of abundance._

### Introduction

This library provides a database abstraction for Swift based on [YapDatabase](https://github.com/yapstudios/YapDatabase),
which in turn is a collection/key/value/metadata store, built atop sqlite.

### Mission

While YapDatabase is a very capable and flexible framework, it has been written with Objective-C's idea of typing in mind – thus
you can almost feel the _friction_ (or _impedance mismatch_, if you're a physicist) when calling it from Swift.

For this first approach, I have strived for a clean syntax and some choices that fit my personal needs. As always, I'm open for any
constructive feedback.

### Usage

```swift
import CornucopiaDB

struct Flights: Codable {

    let number: String
    let source: String
    let destination: String
    let duration: Int

}

let db = CornucopiaDB.Database() // creates a database at a default path
```

… TBD …

### Classes

… TBD …

### Integration

Use it via the Swift Package Manager.

### Warning

This is pre-alpha work, we don't even have a version number.
