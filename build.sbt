name := "scala-hbulk"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Apache Repo" at "http://repo1.maven.org/maven2"

resolvers += "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies +=
    "org.apache.hbase" % "hbase" % "0.90.4-cdh3u3"

libraryDependencies +=
    "org.apache.hbase" % "hbase" % "0.90.4-cdh3u3" % "test"

ivyXML :=
   <dependencies>
     <dependency org="org.apache.hbase" name="hbase" rev="0.90.4-cdh3u3">
       <exclude module="thrift"/>
     </dependency>
   </dependencies>