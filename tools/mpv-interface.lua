local os = require 'os'
local io = require 'io'
local pfx = '/tmp/eb383810f22a-'
local posix = require 'posix'

mp.enable_messages('error')

f = io.open( pfx .. "last-volume", "r" )
if f ~= nil then
  io.input(f)
  volume = io.read()
  vol = mp.set_property('volume', volume)
  io.close(f)
end

local function string_split(str, sep)
   local sep, fields = sep or ":", {}
   local pattern = string.format("([^%s]+)", sep)
   str:gsub(pattern, function(c) fields[#fields+1] = c end)
   return fields
end

function tprint (tbl, indent)
  if not indent then indent = 0 end
  for k, v in pairs(tbl) do
    formatting = string.rep("  ", indent) .. k .. ": "
    if type(v) == "table" then
      print(formatting)
      tprint(v, indent+1)
    elseif type(v) == 'boolean' then
      print(formatting .. tostring(v))      
    else
      print(formatting .. v)
    end
  end
end

function lg(e) 
  tprint(e,2)
  if e.level == "error" and e.prefix == 'stream' then
    print("Exiting...")
    mp.command('quit 1')
  end
end

function print_on_start(e,f)
  pl = mp.get_property_native("playlist", {})
  pos = mp.get_property_native("playlist-pos-1", 1)
  ttl = mp.get_property_native("playlist-count")
  toprint = string.gsub(pl[pos]['filename'], "(.*/)(.*)-%d*.mp3", "%2")
  os.execute('mpv-lib announce "' .. toprint .. '"')
  os.execute('mpv-lib ardy_stat T ' .. pos .. ' ' .. ttl)
end

function openpage_handler()
  os.execute('mpv-lib open_page "' .. mp.get_property('path') .. '"')
end

function getinfo_handler()
  fullpath = mp.get_property('path')
  fullpath = fullpath:gsub('/%./', "/")
  parts = string_split(fullpath, '/')
  partial = parts[#parts - 2] .. '/' .. parts[#parts - 1] 
  cmd = 'mpv-lib _info "' .. table.concat({
    partial, 
    mp.get_property('working-directory'),
    mp.get_property('path')
  }, '" "') .. '"'

  -- print( mp.get_property('path'), cmd ) 
  os.execute( cmd )
  print( mp.get_property_native('media-title') )
end

function record_volume()
  vol = mp.get_property('volume')
  f = io.open( pfx .. "last-volume", "w" )
  io.output(f)
  io.write(vol)
  io.close(f)
end

function share_handler()
  print("Sharing ... ")
  fullpath = mp.get_property('path')
  fullpath = fullpath:gsub('/%./', "/")
  cmd = 'mpv-lib _trinfo "' .. table.concat({
    mp.get_property('working-directory') .. '/' .. fullpath,
    mp.get_property('duration')
  }, '" "') .. '"'
  os.execute( cmd )
end

--
-- exit codes
--
--  0-4       - reserved for mpv
--  5         - keyboard exit
--  6         - skip
--  7         - quit and reprompt
--  8         - purge
--  13 .. 15  - score
--
-- known as player_exit in mpv-once
--

function quit_handler(level)
  mp.command('quit ' .. level)
end

mp.register_event("log-message", lg)
mp.register_event("file-loaded", print_on_start)
mp.register_event("shutdown", record_volume)
mp.register_script_message('updatearduino', print_on_start)
mp.add_key_binding('f', 'share', share_handler)
mp.add_key_binding('o', 'openpage', openpage_handler)
mp.add_key_binding('?', 'getinfo', getinfo_handler)
-- We call this a normal quit but we want to differentiated it 
-- from the quit on track end
mp.add_key_binding('q', 'quit', function() 
  quit_handler(5)
end)
mp.add_key_binding('Q', 'prompt-quit', function() 
  quit_handler(7)
end)

mp.add_key_binding('s', 'skipone', function()
  mp.command('quit 6')
end)
mp.add_key_binding('S', 'skip', function()
  mp.command('quit 6')
end)
mp.add_key_binding('P', 'purge', function()
  mp.command('quit 8')
end)

mp.add_key_binding('e', 'env', function() 
  for i, s in pairs(posix.getenv()) do
    print(i, s)
  end
end)

for i=2,5 do
  mp.add_key_binding(tostring(i), 'pl-' .. i, function() 
    mp.command('quit ' .. (10 + i))
  end)
end
