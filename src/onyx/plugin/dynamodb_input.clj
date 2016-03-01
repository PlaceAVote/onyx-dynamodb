(ns onyx.plugin.dynamodb-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [clojure.core.async :refer [timeout alts!! chan put!]]
            [taoensso.timbre :refer [debug info] :as timbre]
            [hildebrand.channeled :refer [scan!]]))

(defn inject-into-eventmap
  [event lifecycle]
  (let [pipeline (:onyx.core/pipeline event)]
    {:dynamodb/pending-messages (:pending-messages pipeline)
     :dynamodb/drained?         (:drained? pipeline)}))

(def reader-calls 
  {:lifecycle/before-task-start inject-into-eventmap})

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
            messages)))

(defrecord ExampleInput [max-pending batch-size batch-timeout pending-messages drained? results-channel]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          timeout-chan (timeout batch-timeout)
          batch (->>
                  (range max-segments)
                  (map (fn [_]
                         (let [[v p] (alts!! [results-channel timeout-chan] :priority true)]
                           (if (and (= p results-channel) (nil? v))
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
        drained? (atom false)
        results-channel (scan! (:dynamodb/config task-map) (:dynamodb/table task-map) {} {:chan (chan batch-size)})]
    (->ExampleInput max-pending batch-size batch-timeout pending-messages drained? results-channel)))
