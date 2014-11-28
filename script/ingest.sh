#!/bin/bash

rbenv_root="/usr/local/rbenv"
export RBENV_ROOT="$rbenv_root"

if [ -n "$rbenv_root" ]; then
  export PATH="${rbenv_root}/bin:$PATH"
  eval "$(rbenv init -)"
fi

DIR=$( cd "$( dirname "$0" )" && pwd )

$DIR/ingest
