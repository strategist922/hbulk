name := "scala-hbulk"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Apache Repo" at "http://repo1.maven.org/maven2"

resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "Temp thrift Repo" at "http://people.apache.org/~rawson/repo/"

libraryDependencies +=
    "org.apache.hbase" % "hbase" % "0.90.6-cdh3u4"

libraryDependencies +=
    "org.apache.hbase" % "hbase" % "0.90.6-cdh3u4" % "test"

libraryDependencies +=
    "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4"