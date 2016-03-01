(ns onyx.plugin.dynamodb-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.dynamodb-input]
            [onyx.api]
            [hildebrand.core :refer [ensure-table!! ensure-table! batch-write-item!! batch-write-item!]]))

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

(ensure-table!! client-opts
  {:table      :people
   :throughput {:read 1 :write 1}
   :attrs      {:id :number}
   :keys       [:id]})

(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Derek"}
   {:id 5 :name "Kristen"}])

(batch-write-item!!
  client-opts
  {:put {:people people}})

(def catalog
  [{:onyx/name :scan
    :onyx/plugin :onyx.plugin.dynamodb-input/input
    :dynamodb/operation :scan
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

(def workflow [[:scan :out]])

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :scan
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

(def results (take-segments! out-chan))

(deftest testing-output
  (testing "Input is received at output"
    (let [expected (set people)]
    (is (= expected (set (butlast results))))
    (is (= :done (last results))))))


(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
