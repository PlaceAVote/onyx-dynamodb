(ns onyx.plugin.dynamodb-stream-test
  (:require [clojure.core.async :refer [chan >!! timeout <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.dynamodb-input]
            [onyx.api]
            [hildebrand.core :refer [ensure-table!! delete-table!! batch-write-item!!]]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(def batch-size 20)
(def batch-timeout 3000)

;;Dynamo table setup.  Requires local instance of dynamodb running on port 8000.
(def client-opts
  {:access-key "<<ACCESS_KEY>>"
   :secret-key "<<SECRET_KEY>>"
   :endpoint "http://127.0.0.1:8000"})


(delete-table!! client-opts :people)
(ensure-table!! client-opts
  {:table                :people
   :throughput           {:read 1 :write 1}
   :attrs                {:id :number}
   :keys                 [:id]
   :stream-specification {:stream-enabled true :stream-view-type :new-and-old-images}})

(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Derek"}
   {:id 5 :name "Kristen"}])

(def expected-output
  [{:event-type :insert :new {:id 1 :name "Mike"}}
   {:event-type :insert :new {:id 2 :name "Dorrene"}}
   {:event-type :insert :new {:id 3 :name "Benti"}}
   {:event-type :insert :new {:id 4 :name "Derek"}}
   {:event-type :insert :new {:id 5 :name "Kristen"}}])

(batch-write-item!!
  client-opts
  {:put {:people people}})

(def catalog
  [{:onyx/name :stream-in
    :onyx/plugin :onyx.plugin.dynamodb-input/input
    :dynamodb/operation :stream
    :onyx/type :input
    :onyx/medium :dynamodb
    :dynamodb/config client-opts
    :dynamodb/table :people
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/max-peers 1
    :onyx/doc "Scans entireity of Peoples table"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:stream-in :out]])

(def results-chan (chan n-messages))
(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(defn inject-in-ch [event lifecycle]
  {:dynamodb/in-chan results-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :stream-in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :stream-in
    :lifecycle/calls :onyx.plugin.dynamodb-input/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 2 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

;;Kill stream processing
(do
  (<!! (timeout 5000))
  (>!! results-chan :done))

(def results (take-segments! out-chan))

(deftest testing-output
  (testing "Input is received at output"
    (let [expected (set expected-output)]
    (is (= expected (set (butlast results))))
    (is (= :done (last results))))))


(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
