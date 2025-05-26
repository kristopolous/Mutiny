function quote(str) {
  gsub(/"/, "\"\"", str);  
  return "\"" str "\"";    
}
BEGIN {
  album=""
  ix=0
}
{
  OFS=",";
  date=$1
  formatted_date = substr(date, 1, 4) "-" substr(date, 5, 2) "-" substr(date, 7, 2) " 00:00:00"
  path=substr($0, 9);
  
  if (album == $6) {
    ix++;
  } else {
    ix=0;
    album=$6
  }

  print quote($5), quote($6), quote($7), ix, quote(path), quote(formatted_date)
}
