# Although anything found in lib.sh can be overridden, here's some pretty useful ones
#
#   "false" is leaving it blank
#   "true" is making it non-blank
#
# Many of these things are modifiable through the REPL, run help to see
# the current state.
#
# Also they can be passed on the command line. For instance
#
#  $ DEBUG=1 NONET=1 mutiny 
#

# If set, won't try to use the internet
# NONET=

# If set, won't do "expensive" network file system operations such as configuring
# new playlists and other things that make sshfs/nfs drag
# NOSCAN=

# If set, don't copy the tracks for an undo of a purge (removal).
# Over network storage, undo will copy it locally which is expensive
# NOUNDO=

# If left unset, will use the localhost. This is in case you want to
# instrument the player running from a different machine
# HOST=

## But wait, there's more! ##
#
# Look at the top of lib.sh.
# Any of those. Yes.
#
#############################
#FMT=mp3
#PLAYER=mpv
#BACKUPDIR=$HOME/.mutiny
#UNDODIR=$tmp/undo
