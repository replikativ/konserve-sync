(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'io.replikativ/konserve-sync)
(def major 0)
(def minor 1)
(defn commit-count [] (b/git-count-revs nil))
(defn version [] (format "%d.%d.%s" major minor (commit-count)))

(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(defn jar-file [] (format "target/%s-%s.jar" (name lib) (version)))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (let [version (version)]
    (println "Building version:" version)
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :scm {:url "https://github.com/replikativ/konserve-sync"
                        :connection "scm:git:git://github.com/replikativ/konserve-sync.git"
                        :developerConnection "scm:git:ssh://git@github.com/replikativ/konserve-sync.git"
                        :tag (str "v" version)}
                  :pom-data [[:description "A synchronization layer for konserve key-value stores"]
                             [:url "https://github.com/replikativ/konserve-sync"]
                             [:licenses
                              [:license
                               [:name "Apache License 2.0"]
                               [:url "http://www.apache.org/licenses/LICENSE-2.0"]]]]})
    (b/copy-dir {:src-dirs ["src"]
                 :target-dir class-dir})
    (b/jar {:class-dir class-dir
            :jar-file (jar-file)})))

(defn install [_]
  (jar nil)
  (b/install {:basis basis
              :lib lib
              :version (version)
              :jar-file (jar-file)
              :class-dir class-dir}))

(defn deploy [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact (jar-file)
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
