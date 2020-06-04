(ns gloss.data.bytes-test
  (:require
   [gloss.data.bytes :as sut]
   [gloss.core.formats :refer [to-buf-seq to-byte-buffer]]
   [clojure.test :refer [deftest is]])
  (:import [java.nio ByteBuffer]))

(defn byte-seq [^ByteBuffer buf]
  (let [buf (sut/duplicate buf)]
    (lazy-seq
     (when (.hasRemaining buf)
       (cons (.get buf) (byte-seq buf))))))

(deftest test-drop-bytes
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 5 (range 100))))]
    (dotimes [i 101]
      (is (= (drop i (range 100)) (mapcat byte-seq (sut/drop-bytes bufs i)))))))

(deftest test-take-bytes
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 5 (range 100))))]
    (dotimes [i 101]
      (is (= (take i (range 101)) (mapcat byte-seq (sut/take-bytes bufs i)))))))

(deftest test-take-contiguous-bytes
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 5 (range 100))))]
    (dotimes [i 101]
      (is (= (take i (range 100)) (byte-seq (sut/take-contiguous-bytes bufs i)))))))

(defn- test-split [split-location skip-bytes source split]
  (let [s (mapcat byte-seq source)
        split (map #(mapcat byte-seq %) (rest split))]
    (if (<= split-location (count s))
      (is
       (= [(take split-location s) (drop (+ split-location skip-bytes) s)] split)
       (with-out-str (prn split-location skip-bytes (map byte-seq source))))
      (is (= [() s] split)))))

(defn take-delimited-bytes [buf-seq delimiters strip-delimiters?]
  (let [f (sut/delimited-bytes-splitter delimiters strip-delimiters?)]
    (f buf-seq)))

(deftest test-take-delimited-bytes
  ;;single-byte delimiters
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 3 (range 12))))]
    (dotimes [i 12]
      (let [delimiters [(to-byte-buffer [i])]]
        (test-split i 1 bufs (take-delimited-bytes bufs delimiters true))
        (test-split (inc i) 0 bufs (take-delimited-bytes bufs delimiters false)))))

  ;;non-existent delimiters
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 4 (range 100))))]
    (test-split 101 0 bufs (take-delimited-bytes bufs [(to-byte-buffer [1 3])] false))
    (test-split 101 0 bufs (take-delimited-bytes bufs [(to-byte-buffer [101])] false)))

  ;;multi-byte delimiters
  (let [bufs (to-buf-seq (map to-byte-buffer (partition 1 (range 15))))]
    (dotimes [i 11]
      (let [delimiters (map #(to-byte-buffer (range i (+ i %))) (range 1 5))]
        (test-split (+ i 4) 0 bufs (take-delimited-bytes bufs delimiters false))
        (test-split i 4 bufs (take-delimited-bytes bufs delimiters true))))))
