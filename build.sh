#!/bin/bash

app=$APPLICATION
env=$TRACK

echo "Building ${app} for ${env}."

cp ~/.ssh/id_rsa id_rsa
cp ~/.ssh/id_rsa.pub id_rsa.pub