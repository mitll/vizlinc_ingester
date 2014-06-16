@echo off

if "%1"=="" (
    echo "Usage: %0 <groovy script> <args> ..."
    exit
)

set PROGRAM=%1
if not exist "%PROGRAM%" (
    echo "Not found:" $PROGRAM
    exit
)

set GROOVY=C:\Users\%USERNAME%\Desktop\Java\groovy-1.8.9
set GREMLIN_JARS=C:\Users\%USERNAME%\Desktop\Java\gremlin-groovy-2.4.0\lib\*
set VIZLINCDB_JARS=C:\Users\%USERNAME%\Documents\NetBeansProjects\vizlincdb\target\*
set VIZLINCDB_LIB_JARS=C:\Users\%USERNAME%\Documents\NetBeansProjects\vizlincdb\target\lib\*

set LIB_JARS=lib\*
set GROOVY_ALL_JAR=%GROOVY%\embeddable\groovy-all-1.8.9.jar
set SRC=src

REM GROOVY_ALL_JAR should be before GREMLIN_JARS. GREMLIN_JARS includes the non-"all" groovy jar 
set CP=%SRC%;%GROOVY_ALL_JAR%;%GREMLIN_JARS%;%LIB_JARS%;%VIZLINCDB_JARS%;%VIZLINCDB_LIB_JARS%

set GC_DEBUG=0
if "%GC_DEBUG"=="1" (
    set JAVA_OPTS="-d64 -server -Xmx2g -XX:+UseConcMarkSweepGC -XX:+PrintGCDetails"
) else (
    set JAVA_OPTS="-d64 -server -Xmx2g -XX:+UseConcMarkSweepGC"
)

REM Get the rest of the args into a single variable.
shift
set REMAINING_ARGS=
:start
  if "%1"=="" goto done
  set REMAINING_ARGS=%REMAINING_ARGS% %1
  shift
  goto start
:done

java -cp %CP% groovy.ui.GroovyMain "%PROGRAM%" %REMAINING_ARGS%
