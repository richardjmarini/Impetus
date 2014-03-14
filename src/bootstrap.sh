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
current_dir=`pwd`;
install_dir="$current_dir/Impetus";
log_file="$current_dir/bootstrap";

#----------------------------------------
# Template variables replaced by DFS
#----------------------------------------
# $deploy_key
# $queue
# $dfs
# $s3
# $mpps

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

# install the Impetus deploy key and config file
echo "installing deploy keys"
execute "echo $deploy_key > /root/.ssh/impetus_rsa" "$log_file";
execute "cat $ssh_config > /root/.ssh/config" "$log_file";
execute "chmod 600 /root/.ssh/*" "$log_file";

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
cmd="./impetusnode.py --queue=$queue --dfs=$dfs --mpps=$mpps start";

execute "$cmd" "$log_file";
