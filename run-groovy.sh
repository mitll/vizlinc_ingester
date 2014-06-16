#!/bin/bash

# Set the exit status to the first non-zero status in a pipeline, so if any command in the pipe fails, we'll know.
set -o pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage:" $_ "<groovy script> <args> ..."
    exit 1
fi
PROGRAM=$1
PROGRAM_BASENAME=$(basename ${PROGRAM} .groovy)
shift

TIMESTAMP=`date +'%Y%m%d-%H%M%S'`

if [[ ! -e $PROGRAM ]]; then
    echo "Not found:" $PROGRAM
    exit 1
fi

OS=$(uname -o)
if [[ ${OS} == "Cygwin" ]] ; then PLATFORM=Cygwin; fi
GROOVY=${HOME}/code/groovy-1.8.9
GREMLIN_JARS=${HOME}'/code/gremlin-groovy-2.4.0/lib/*'
VIZLINCDB_JARS='../vizlincdb/target/*'
VIZLINCDB_LIB_JARS='../vizlincdb/target/lib/*'

if [[ ${OS} == "Cygwin" ]] ;
then
    GROOVY=C:/Users/${USER}/Desktop/Java/groovy-1.8.9
    GREMLIN_JARS=C:/Users/${USER}'/Desktop/Java/gremlin-groovy-2.4.0/lib/*'
    VIZLINCDB_JARS=C:/Users/${USER}'/Documents/NetBeansProjects/vizlincdb/target/*'
    VIZLINCDB_LIB_JARS=C:/Users/${USER}'/Documents/NetBeansProjects/vizlincdb/target/lib/*'
fi

LIB_JARS='lib/*'
GROOVY_ALL_JAR=${GROOVY}/embeddable/groovy-all-1.8.9.jar

SRC='src'
# GROOVY_ALL_JAR should be before GREMLIN_JARS. GREMLIN_JARS includes the non-"all" groovy jar 
CP=${SRC}:${GROOVY_ALL_JAR}:${GREMLIN_JARS}:${LIB_JARS}:${VIZLINCDB_JARS}:${VIZLINCDB_LIB_JARS}:${CLASSPATH}

# Change path delimiters to ';' on Windows
if [[ ${OS} == "Cygwin" ]] ;
then
    # Must quote due to semicolons.
    CP="${SRC};${GROOVY_ALL_JAR};${GREMLIN_JARS};${LIB_JARS};${VIZLINCDB_JARS};${VIZLINCDB_LIB_JARS};${CLASSPATH}"
fi

GC_DEBUG=0
if [ "${GC_DEBUG}" = "1" ] ; then
	 export JAVA_OPTS="-d64 -server -Xmx12g -XX:+UseConcMarkSweepGC -XX:+PrintGCDetails"
else
	 export JAVA_OPTS="-d64 -server -Xmx12g -XX:+UseConcMarkSweepGC"
fi

mkdir -p logs
# Use pipefail (see above) so failing status will be caught.

# The groovy command uses the groovy jar, not the groovy-all jar, so the other jars it needs (from groovy's lib directory) may conflict.
# Avoid this problem by running the GroovyMain and using the groovy-all jarjar that includes everything groovy depends on.
java -cp ${CP} groovy.ui.GroovyMain ${PROGRAM} "$@" | tee logs/${PROGRAM_BASENAME}.${TIMESTAMP}.txt
