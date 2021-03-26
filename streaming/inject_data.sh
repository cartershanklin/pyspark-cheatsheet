#!/bin/sh

rm -f input/*
for f in 70 71 72 73 74 75 76 77 78 79 80; do
    cp chunks/auto-mpg-chunk$f.csv input
    sleep 25
done
