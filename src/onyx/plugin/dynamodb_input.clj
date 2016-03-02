(ns onyx.plugin.dynamodb-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [clojure.core.async :refer [timeout alts!! chan put!]]
            [taoensso.timbre :refer [debug info] :as timbre]
            [onyx.plugin.functions :refer [<results-channel]]))

(defn inject-into-eventmap
  [event lifecycle]
  (when-not (:dynamodb/in-chan event)
    (throw (ex-info ":dynamodb/in-chan not found - add it using a :before-task-start lifecycle"
             {:event-map-keys (keys event)})))

  (let [pipeline (:onyx.core/pipeline event)
        task-map (:onyx.core/task-map event)
        results-chan (<results-channel (:dynamodb/operation task-map) (:dynamodb/config task-map)
                       (:dynamodb/table task-map) (:dynamodb/in-chan event))]
    {:dynamodb/pending-messages (:pending-messages pipeline)
     :dynamodb/drained?         (:drained? pipeline)
     :dynamodb/results-chan results-chan}))

(def reader-calls 
  {:lifecycle/before-task-start inject-into-eventmap})

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
            messages)))

(defrecord DynamoConsumer [max-pending batch-size batch-timeout pending-messages drained?]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ {:keys [dynamodb/results-chan] :as event}]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          timeout-chan (timeout batch-timeout)
          batch (->>
                  (range max-segments)
                  (map (fn [_]
                         (let [[v p] (alts!! [results-chan timeout-chan] :priority true)]
                           (if (or (= v :done) (and (= p results-chan) (nil? v)))
                             :done
                             v))))
                  (remove (comp nil?))
                  (mapv (fn [message]
                          (t/input (java.util.UUID/randomUUID) message))))]

      (if (and
            (all-done? (vals @pending-messages))
            (all-done? batch)
            (or (not (empty? @pending-messages)) (not (empty? batch))))
        (reset! drained? true)
        (reset! drained? false))

      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      {:onyx.core/batch batch}))
  (seal-resource [this event])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (swap! pending-messages dissoc segment-id))

  (retry-segment 
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ _]
    @drained?))

(defn input [event]
  (let [task-map (:onyx.core/task-map event)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        pending-messages (atom {})
        drained? (atom false)]
    (->DynamoConsumer max-pending batch-size batch-timeout pending-messages drained?)))
