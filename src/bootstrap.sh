#!/bin/bash
#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: bootstrap
# Desciption: Processing node bootstrap for the Impetus Framework.
#
# License:
#    Impetus is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 2 of the License, or
#    any later version.
#
#    Impetus is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Impetus.  If not, see <http://www.gnu.org/licenses/>.
#---------------------------------------------------------------------------
current_dir="/home/ubuntu";

install_dir="$current_dir/Impetus";
log_file="$current_dir/bootstrap";

#----------------------------------------
# Template variables replaced by DFS
#----------------------------------------
# deploykey
# queue
# qport
# dfs
# dport
# s3
# mpps

#----------------------------------------
# helper functions
#----------------------------------------
function execute {
   _cmd=$1;
   _logfile=$2;
   echo "executing $_cmd" >> $_logfile.out;
   $_cmd > $_logfile.out 2> $_logfile.err;
   rc=$?;
   if [[ $rc -ne 0 ]];
   then
      echo "ERROR: could not execute [$_cmd]" >> $_logfile.err;
      exit -1;
   fi;
}

function executebg {
   _cmd=$1;
   _logfile=$2;
   echo "executing $_cmd" >> $_logfile.out;
   $_cmd >$_logfile.out 2> $_logfile.err &
   rc=$?;
   if [[ $rc -ne 0 ]];
   then
      echo "ERROR: could not execute [$_cmd]" >> $_logfile.err;
      exit -1;
   fi;
}

function getrepo {
   username=$1;
   hostname=$2;
   reponame=$3;

   cmd="git clone $username@$hostname:$reponame";
   execute "$cmd" "$log_file";
}

#----------------------------------------
# install other packages
#----------------------------------------

#----------------------------------------
# install Impetus deploy keys and repo
#----------------------------------------
cd $install_dir;

echo "installing deploy key and ssh_config file";

echo "# Impetus
   Host github.com-Impetus
   Hostname github.com
   User git
   PreferredAuthentications publickey
   IdentityFile ~/.ssh/impetus_rsa
   IdentitiesOnly yes
" > /root/.ssh/config;

echo "$deploykey" > /root/.ssh/impetus_rsa;

execute "chmod 600 /root/.ssh/impetus_rsa" "$log_file";

echo "cloning Impetus";
getrepo "git" "github.com-Impetus" "richardjmarini/Impetus.git";


#----------------------------------------
# startup virtual frame buffer -- used for selenium requests
#----------------------------------------
#export DISPLAY=:99;
#cmd="Xvfb :99 -screen 0 800x600x16";
#echo $cmd;
#executebg "$cmd" "$log_file";

#----------------------------------------
# startup Impetus Node
#----------------------------------------
echo "starting Impetus Node";

cd $install_dir/Impetus/src;
cmd="./impetusnode.py --queue=$queue --qport=$qport --dfs=$dfs --dport=$dport --mpps=$mpps start";

execute "$cmd" "$log_file";
