#!/usr/bin/env bash

code_dir=$(basename `pwd`)
cd ..

dir=$(mktemp -d)
cp $code_dir/* $dir -r
cd $dir
rm .git -rf
rm .idea -rf
rm go.mod go.sum -f
rm LICENSE -f
rm README.md -f
rm install_pro.sh
rm goque.test
rm Makefile

find -iname '*.go' -exec sed -i 's@github.com/beeker1121/goque@npd/pkg/goque@' {} \;

rm -rf test_queue ack-design.txt

cd ..
dst=~/pro_code/npd/pkg/goque
mkdir -p $dst
cp $dir/* $dst -r

rm $dir -rf