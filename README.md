[![Stories in Ready](https://badge.waffle.io/Imaginea/KodeBeagle.png?label=ready&title=Ready)](https://waffle.io/Imaginea/KodeBeagle)
# Kodebeagle.

[![Join the chat at https://gitter.im/Imaginea/KodeBeagle](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Imaginea/KodeBeagle?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build](https://travis-ci.org/Imaginea/KodeBeagle.svg?branch=master)](https://travis-ci.org/Imaginea/KodeBeagle/builds)

[Refer to docs](http://imaginea.github.io/KodeBeagle/)

We use [gh-pages](https://pages.github.com/) and Jekyll for demo web ui.

## Installation instructions.

### Step 1 Configuration.
Update the configuration in application.properties

### Step 2 Run.

KodeBeagle is built by [SBT](http://www.scala-sbt.org/)

`$ sbt`

`$ > project core`

`$ > run`

Then select `com.kodebeagle.spark.CreateIndexJob` to run the Spark job that creates index which can be digested by elasticsearch.

### Step 3 Upload to elastic Search.

Please install elasticsearch from [here](http://www.elasticsearch.org/overview/elkdownloads/).
Once the elasticsearch service is running "locally". Run,

`$ bin/upload_to_es.sh`

This will upload the index generated by the job in previous step to elastic search server.

# Kodebeagle Intellij idea plugin

## Installation instructions.

### Step 1 Download and install Intellij Idea.
If you already have intellij installed, you can skip this step.

### Step 2 package the plugin
`sbt -Didea.lib="$IDEA_HOME/lib" package`

### Step 2 Install plugin from disk
Once jar is built goto Settings -> Plugins -> Install from disk -> Select the plugin jar built in previous step-> Restart(to use plugin)
