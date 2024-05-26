#!/bin/bash

echo " * Generating JaCoCo reports"
mvn -P adequacy-controlflow jacoco:report