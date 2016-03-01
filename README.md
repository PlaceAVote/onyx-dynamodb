## onyx-dynamodb

Onyx plugin for dynamodb.

#### Installation

In your project file:

```clojure
[onyx-dynamodb "0.8.2.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.dynamodb])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.dynamodb/input
 :onyx/type :input
 :onyx/medium :dynamodb
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from dynamodb"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.dynamodb/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:dynamodb/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
