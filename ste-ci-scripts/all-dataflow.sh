#!/bin/bash

TESTS=$(find $TESTED_SUBPROJECT/src/test/java -iname '*.java' -type f -printf '%f\n' | sed 's/.java//g' | tr '\n' ' ')

for test in $TESTS; do
  echo " * Data flow adequacy ($test)"
  mvn -P adequacy-dataflow clean test -Dtest=$test -DfailIfNoTests=false -DbaduaVer=$BADUAVER
  bash ste-ci-scripts/genbaduareports.sh
  mkdir -pv ba-dua-reports
  mv ba-dua.xml ba-dua-reports/$test.xml
done