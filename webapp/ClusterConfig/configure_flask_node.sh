#!/bin/bash

# Copyright 2015 Insight Data Science
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


CLUSTER_NAME=webapp-node

peg up master.yml 
peg up workers.yml 

echo '-------------------------------------------------------------'
echo 'enabling the ssh-agent:'
echo '-------------------------------------------------------------'
eval `ssh-agent -s`

echo '-------------------------------------------------------------'
echo 'verify peg installation:'
echo '-------------------------------------------------------------'
peg config

echo '-------------------------------------------------------------'
echo 'Available Regions:'
echo '-------------------------------------------------------------'
aws ec2 --output json describe-regions --query Regions[].RegionName

echo '-------------------------------------------------------------'
echo 'Describe cluster and some kind of initialization'
echo '-------------------------------------------------------------'
peg fetch ${CLUSTER_NAME}

echo '-------------------------------------------------------------'
echo 'Starting installations'
echo '-------------------------------------------------------------'
peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws


