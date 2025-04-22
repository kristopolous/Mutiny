#!/usr/bin/env nodejs

const fs = require('fs');
const tmp = process.env.MUT_TMP || '/tmp/mutiny';
const net = require('net');
const client = net.createConnection(tmp + '/mpvsocket')
const quitList = ['quit', 'pause', 'playback-restart', 'unpause'];
const command_orig = process.argv[2];
const arg = process.argv[3];
const sclient = fs.createWriteStream(`${tmp}/ardy_socket`);
const util = require('util');
var cb = false;
var command = command_orig;
var direction = ['back', 'prev'].includes(command) ? -1 : 1;
let andthen = () => {};

const commandMap = {
  pause: ['set_property', 'pause', true],
  play: ['set_property', 'pause', false],
  startover: ['set_property', 'time-pos', 0],
  quit: ['quit'],
  test: ['script-message', 'updatearduino'],
  getpause: ['get_property', 'pause']
}

function cleanExit() {
  client.end(() => {
    process.exit(0);
  });
}

function send(list) {
  var towrite = JSON.stringify({command: list});
  client.write(Buffer.from(towrite + '\n', 'utf-8'));
}

if (command == 'pauseplay') {
  cb = function(state) {
    // this is a huge hack. the code below is set to
    // exit after it receives a response from set_property
    // so we get these messages in first. This works
    // reliably because it's a local serialized unix socket.
    if(!state) {
      sclient.write("2..paused..".padEnd(32), 'utf-8',
        () => sclient.end
      );
    } else {
      // restore the song name
      send(['script-message', 'updatearduino']);
    }
    send(['set_property', 'pause', !state ]);
  }
  command = 'getpause';

} else if (['volume'].includes(command)) {
  cb = function(volume) {
    send(['set_property', 'volume', +arg]);
  }
  command = 'volume';
} else if (['volup', 'voldn'].includes(command)) {
  cb = function(volume) {
    let newvol = +volume + (command_orig == 'volup' ? 2 : -2);
    send(['set_property', 'volume', newvol]);
    const bt = Math.floor(Math.min(100,newvol)/100*0xff);
    const binaryData = Buffer.from([0x56,bt]);
    sclient.write(binaryData, () => sclient.end);
  }
  command = 'volume';

} else if (['prev', 'next'].includes(command)) {
  cb = function(pos) {
    let newpos = pos + direction;
    if(newpos < 0) {
      cleanExit();   
    }
    send(['set_property', 'playlist-pos', newpos]);
  }
  command = 'playlist-pos';

} else if (['back', 'forward'].includes(command)) {
  cb = function(pos) {
    // if the user passed a number it to go backward or forward some
    // specific amount
    let amount = parseInt(arg, 10) || 10;
    send(['set_property', 'time-pos', pos + amount * direction]);
  }
  command = 'time-pos';
}

if (!command) {
  console.log(Object.keys(commandMap).concat([
    'back', 'forward', 
    'volume', 'volup', 'voldn',
    'pauseplay', 
    'prev', 'next']).sort());
  cleanExit();   
}

client.on('connect', () => {
  if(commandMap[command]) {
    send(commandMap[command]);
  } else {
    send(['get_property', command]);
  }
});

client.on('data', (data) => {
  data.toString('utf8').trim().split('\n').forEach(rowRaw => {
    let row = JSON.parse(rowRaw);
    console.log(row);

    if (quitList.includes(row.event) || !cb) {
      cleanExit();   
    } 

    if ('data' in row && cb) {
      let mycb = cb;
      cb = null;
      mycb(row.data);
    }
  });
  andthen();
});

