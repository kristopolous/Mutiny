#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# These are things you might need to modify
CODE=$HOME/code/music-explorer
TMP=/tmp/
music_dir=/raid-real/mp3/label/
DB_DIR=$HOME/www/pl

playdb=$DB_DIR/playlist.db
playtxt=$TMP/playlist.txt
playcsv=$TMP/playlist.csv
version=$(cd $CODE;git describe)

start=$(date +%s)
dbg() {
  [[ -n "$INFO" ]] && echo $(( $(date +%s) - start )) "$1"
}
 
gen_playlist() {
  dbg "truncating"
  truncate --size 0 $playtxt
  dbg "recreating"
  cat "$music_dir".listen_done | grep rating_5 | sort | awk -F ' ' ' { print $1" "$NF } ' | while read i dt
  do
    ls "$music_dir"$i/*.mp3  | awk ' { print '$dt'$0 } ' >> $playtxt
  done
  dbg "playlist done"
}

copy_songs() {
  # get just the paths
  dbg "copying songs"
  grep rating_5 "$music_dir".listen_done | sort | cut -f 1 -d ' ' | uniq | awk '{ print "'$music_dir'"$0 }' | while read remotepath; do
    localpath="$(echo "$remotepath" | sed "s/raid-real/raid/")"
    if [[ ! -d "$localpath" ]];then 
      mkdir -p "$localpath"
      cp --preserve=timestamps -ur "$remotepath"/* "$localpath"
    fi
  done

  dbg "copying done"
  sed -i 's/raid-real/raid/g' $playtxt
}

convert_songs() {
  dbg "converting songs"
  truncate --size 0 conv_update.sql
  local n=0
  sqlite3 $playdb "select path from tracks where converted is not true" > conv_list.txt
  cat conv_list.txt | while read p; do
    if [[ ! -s "$p" ]]; then
      echo "path $p doesn't exist"
    else 
      $CODE/tools/mpv-lib toopus "$p" &
      pid_opus=$!

      $CODE/tools/mpv-lib tom5a "$p" &
      pid_m5a=$!

      wait $pid_opus
      wait $pid_m5a

      path_m5a="${p/.mp3/.m5a}"
      path_opus="${p/.mp3/.opus}"

      if [[ -s "$path_m5a" && -s "$path_opus" ]]; then
        (( n++ ))
        echo "update tracks set converted=true where path = \"$p\";" >> conv_update.sql
      else
        echo "Error for $p:"
        echo " - ($path_m5a)"
        echo " - ($path_opus)"
      fi
      if (( n > 80 )); then
        echo ""
        sqlite3 $playdb < conv_update.sql
        truncate --size 0 conv_update.sql
        n=0
      fi
    fi
  done
  sqlite3 $playdb < conv_update.sql
  truncate --size 0 conv_update.sql
  dbg "converting done"
}

import_todb() {
  fields="label,release,track,track_ix,path,created_at"
  cat $playtxt | awk -F '/' -f parser.awk > $playcsv
  {
  sqlite3 $playdb << ENDL
PRAGMA encoding=UTF8;
.mode csv
delete from tracks_temp;
.import $playcsv tracks_temp
insert or ignore into tracks(label,release,track,track_ix,path,created_at) select * from tracks_temp;
update tracks set version = "$version" where version is null;
ENDL
} #>& /dev/null
}

create_db() {
  truncate --size 0 $playdb
  rm $playdb
  sqlite3 $playdb << ENDL
  create table tracks_temp(label text, release text, track text, track_ix int, path text, created_at date); 
  create table tracks(
    id INTEGER PRIMARY KEY,
    label text,
    release text,
    track text,
    track_ix int,
    path text,
    created_at date,
    version text default "",
    listen integer default 0,
    plays integer default 0,
    duration integer default 0,
    converted boolean default false,
    unique(path));
create index label_name on tracks(label);
create index release_name on tracks(release);
ENDL
}
create_db
gen_playlist
copy_songs
import_todb
#convert_songs
