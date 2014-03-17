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
project_name="Impetus";
current_dir="/home/ubuntu";

install_dir="$current_dir/${project_name}";
virtualenv_dir="$current_dir/envs";
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
   operation=$1;
   username=$2;
   hostname=$3;
   reponame=$4;

   if [ $operation = "pull" ]
   then
      cd $install_dir;
   fi

   cmd="git $operation $username@$hostname:$reponame";

   execute "$cmd" "$log_file";
}

#----------------------------------------
# install other packages
#----------------------------------------

#----------------------------------------
# install deploy keys and repo
#----------------------------------------
echo "installing deploy key and ssh_config file";

echo "# ${project_name}
   Host github.com-${project_name}
   StrictHostKeyChecking no
   Hostname github.com
   User git
   PreferredAuthentications publickey
   IdentityFile ~/.ssh/impetus_rsa
   IdentitiesOnly yes
" > /root/.ssh/config;

echo "$deploykey" > /root/.ssh/impetus_rsa;

execute "chmod 600 /root/.ssh/impetus_rsa" "$log_file";

echo "cloning ${project_name}";
getrepo "pull" "git" "github.com-${project_name}" "richardjmarini/${project_name}.git";


#----------------------------------------
# startup virtual frame buffer -- used for selenium requests
#----------------------------------------
#export DISPLAY=:99;
#cmd="Xvfb :99 -screen 0 800x600x16";
#echo $cmd;
#executebg "$cmd" "$log_file";

#----------------------------------------
# switch to the virtual env
#----------------------------------------
source $virtualenv_dir/${project_name}/bin/activate

#----------------------------------------
# startup daemon process
#----------------------------------------
echo "starting ${project_name} Node";

cd $install_dir/src;
cmd="./impetusnode.py --queue=$queue --qport=$qport --dfs=$dfs --dport=$dport --mpps=$mpps start";

execute "$cmd" "$log_file";
exit(0);
