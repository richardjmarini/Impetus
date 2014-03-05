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

installDir="/home/rmarini";
git="/usr/bin/git";
pip="/usr/bin/pip";
logFile="/home/rmarini/bootstrap";

queue="%(queue)s";
dfs="%(dfs)s";
s3="%(s3)s";
mpps=%(mpps)s;

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

function getRepo {
   username=$1;
   hostname=$2;
   reponame=$3;

   cmd="$git clone $username@$hostname:$reponame";
   execute "$cmd" "$logFile";
}

#----------------------------------------
# install other packages
#----------------------------------------

#----------------------------------------
# install Impetus repo
#----------------------------------------
cd $installDir;

echo "cloning Impetus";
getRepo "git" "github.com-Impetus" "richardjmarini/Impetus.git";

#----------------------------------------
# setup deploy keys for other repos 
#----------------------------------------
cd $installDir;

# install deploy keys for repos
execute "cp Impetus/keys/* /root/.ssh/" "$logFile";
execute "chmod 600 /root/.ssh/*" "$logFile";
execute "chmod 644 /root/.ssh/*.pub" "$logFile";

#----------------------------------------
# install other repos
#----------------------------------------
echo "cloning VectorSpaceSearchEngine"
getRepo "git" "github.com-VectorSpaceSearchEngine" "richardjmarini/VectorSpaceSearchEngine.git";

#----------------------------------------
# startup virtual frame buffer -- used for selenium requests
#----------------------------------------
export DISPLAY=:99;
cmd="Xvfb :99 -screen 0 800x600x16";
echo $cmd;
executebg "$cmd" "$logFile";

#----------------------------------------
# startup Impetus Node
#----------------------------------------
echo "starting Impetus Node";

cd $installDir/Impetus/src;
cmd="./impetusnode.py --queue=$queue --dfs=$dfs --pidDir=../pid --logDir=../log --mpps=$mpps --s3=$s3 start";

execute "$cmd" "$logFile";
