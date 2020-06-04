;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
 gloss.core.structure
  (:require
   [clojure.walk :as walk]
   [gloss.core.protocols :as p]
   [gloss.data.primitives :as prim]
   [gloss.data.bytes :as bytes]))

;;;

(defn- sequence-reader
  [result readers]
  (reify p/Reader
    (read-bytes [_ b]
      (loop [b b, s readers, result result]
        (if (empty? s)
          [true result b]
          (let [[reader? x] (first s)]
            (if-not reader?
              (recur b (rest s) (conj result x))
              (let [[success x b] (p/read-bytes x b)]
                (if success
                  (recur b (rest s) (conj result x))
                  [false
                   (p/compose-callback
                    x
                    #(p/read-bytes
                      (sequence-reader (conj result %1) (rest s))
                      %2))
                   b])))))))))

(defn convert-sequence
  [frame]
  (let [finite-frame? (every? p/sizeof (filter p/reader? frame))
        s             (map list (map p/reader? frame) frame)
        codecs        (filter p/reader? frame)
        sizeof        (when finite-frame? (apply + (map p/sizeof codecs)))
        reader        (sequence-reader [] s)]
    (reify
      p/Reader
      (read-bytes [this b]
        (if (and sizeof (< (bytes/byte-count b) sizeof))
          [false this b]
          (p/read-bytes reader b)))
      p/Writer
      (sizeof [_] sizeof)
      (write-bytes [_ buf vs]
        (when-not (sequential? vs)
          (throw (Exception. (str "Expected a sequence, but got " vs))))
        (if finite-frame?
          (p/with-buffer [buf sizeof]
            (doseq [[[_ x] v] (filter ffirst (map list s vs))]
              (p/write-bytes x buf v)))
          (apply concat
                 (map
                  (fn [[[_ x] v]] (p/write-bytes x buf v))
                  (filter ffirst (map list s vs)))))))))

(defn convert-map
  [frame]
  (let [ks         (sort (keys frame))
        vs         (map frame ks)
        codec      (convert-sequence vs)
        read-codec (p/compose-callback
                    codec
                    (fn [x b]
                      [true (zipmap ks x) b]))]
    (reify
      p/Reader
      (read-bytes [_ b]
        (p/read-bytes read-codec b))
      p/Writer
      (sizeof [_]
        (p/sizeof codec))
      (write-bytes [_ buf v]
        (when-not (map? v)
          (throw (Exception. (str "Expected a map, but got " v))))
        (p/write-bytes codec buf (map v ks))))))

(defn- compile-frame- [f]
  (cond
    (map? f) (convert-map (zipmap (keys f) (map compile-frame- (vals f))))
    (sequential? f) (convert-sequence (map compile-frame- f))
    :else f))

(defn compile-frame
  "Takes a frame, and returns a codec.  This function is idempotent; passing in a codec
   is a safe operation.

   Functions that transform the values after they are decoded and before they are encoded
   can be specified, which allows the frame to only be an intermediate representation of
   the final Clojure data structure."
  ([frame]
   (if (p/reader? frame)
     frame
     (->> frame
          (walk/postwalk-replace prim/primitive-codecs)
          compile-frame-)))
  ([frame pre-encoder post-decoder]
   (let [codec      (compile-frame frame)
         read-codec (p/compose-callback
                     codec
                     (fn [x b]
                       [true (post-decoder x) b]))]
     (reify
       p/Reader
       (read-bytes [_ b]
         (p/read-bytes read-codec b))
       p/Writer
       (sizeof [_]
         (p/sizeof codec))
       (write-bytes [_ buf v]
         (p/write-bytes codec buf (pre-encoder v)))))))
