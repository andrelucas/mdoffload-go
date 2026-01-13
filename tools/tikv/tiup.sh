#!/bin/bash

curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
PATH=~/.tiup/bin:$PATH
hash -r
set -e -x
tiup update --self
tiup update playground

