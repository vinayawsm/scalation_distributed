////////////////////////////////////////////////////////////////////////////////
// build.sbt - specifications for Simple Build Tool
// https://www.scala-sbt.org/1.x/docs
// https://www.scala-sbt.org/1.x/docs/Forking.html

lazy val commonSettings = Seq (

  name         := "scalation",
  organization := "scalation",
  version      := "1.5",
  scalaVersion := "2.12.4",
  fork         := true,
  connectInput := true,

  concurrentRestrictions := Seq(Tags.limitAll(1)),

  ////////////////////////////////////////////////////////////////////////////////
  // Scala options

  scalacOptions += "-deprecation",
  scalacOptions += "-feature",
  scalacOptions += "-Xfatal-warnings",
  scalacOptions += "-opt:l:method",               // enable all intra-method optimizations
  scalacOptions += "-opt:l:inline",               // enable cross-method optimizations
  scalacOptions += "-opt-inline-from:**",         // allow inlining for all classes
  scalacOptions += "-opt-warnings",
  scalacOptions += "-Xlint:-adapted-args",        // run lint - disable "adapted-args" (auto tupling used)
  // scalacOptions += "-feature",
  // scalacOptions += "-unchecked",

  ////////////////////////////////////////////////////////////////////////////////
  // Java options

  javaOptions += "-Xmx2G"

) // commonSettings

lazy val root = (project in file("."))

  .settings (
    commonSettings,
    name := "scalation_dist",

    ////////////////////////////////////////////////////////////////////////////////
    // Scala Modules
    // @see http://scala-lang.org/documentation/api.html

    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.14",
    libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.5.14",
    libraryDependencies += "com.typesafe.akka" %% "akka-cluster-tools" % "2.5.14",
    libraryDependencies += "com.typesafe.akka" %% "akka-distributed-data" % "2.5.14",
    libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.14",
    libraryDependencies += "com.typesafe.akka"          %% "akka-persistence" % "2.5.18",
    libraryDependencies += "org.iq80.leveldb"            % "leveldb"          % "0.7",
    libraryDependencies += "org.fusesource.leveldbjni"   % "leveldbjni-all"   % "1.8",

    ////////////////////////////////////////////////////////////////////////////////
    // Unit Testing

    libraryDependencies += "junit" % "junit" % "4.11" % "test",
    libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default",

    ////////////////////////////////////////////////////////////////////////////////
    // Neo4j Driver
    // @see https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver

    libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.4.4"

  ) // root

////////////////////////////////////////////////////////////////////////////////
// Fast Regex

// libraryDependencies += "dk.brics.automaton" % "automaton" % "1.11-8"

////////////////////////////////////////////////////////////////////////////////
// Java HTML Parser - needed for UCIML

// libraryDependencies += "org.jsoup" % "jsoup" % "1.8.2"


