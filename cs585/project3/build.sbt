ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
	.settings(
		name := "project3",
		// Emit Java 8 bytecode for .java sources (container runs Java 8).
		// --release exists only in javac 9+; javac 8 needs -source/-target instead.
		javacOptions ++= {
			if (sys.props("java.specification.version").startsWith("1.")) Seq("-source", "1.8", "-target", "1.8")
			else Seq("--release", "8")
		},
		Test / fork := true,
		Test / parallelExecution := false,
		Test / javaOptions += "-Xmx1g",
		libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-core" % "3.5.6" % Provided,
			"org.apache.spark" %% "spark-sql" % "3.5.6" % Provided,
			"org.scalatest" %% "scalatest" % "3.2.19" % Test
		)
	)
