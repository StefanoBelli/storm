#!/bin/bash

BA_DUA_CLI=testdeps/ba-dua/ba-dua-cli-${BADUAVER}-all.jar

BA_DUA_CLASSES_PATH=$TESTED_SUBPROJECT/target/classes/
BA_DUA_SER=$TESTED_SUBPROJECT/coverage.ser
BA_DUA_OUTXML=ba-dua.xml

echo " * Generating ba-dua XML report"
java -jar $BA_DUA_CLI report -classes $BA_DUA_CLASSES_PATH -input $BA_DUA_SER -xml $BA_DUA_OUTXML