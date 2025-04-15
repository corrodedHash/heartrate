#!/bin/sh

EVENTS="-e DELETE -e MODIFY -e CREATE -e MOVE"

while ! inotifywait -r $EVENTS /watch; do
    python main.py
done