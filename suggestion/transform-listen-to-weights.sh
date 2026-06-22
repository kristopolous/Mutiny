#!/bin/bash
tail -$1 $2 | grep -Ev '(time:0:0|nothing|skip)' | sed 's/__purge/0/g;s/__rating//g' | cut -d ' ' -f -2

