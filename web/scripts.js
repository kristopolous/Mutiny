var 
  _track = {},
  _my = {},
  _qstr,
  _next = {},
  _db = {},
  _tab = 'track',
  _if,
  _level = 2,
  _DOM = {},
  _lock = {},
  path_to_url = str => 'https://bandcamp.com/EmbeddedPlayer/size=large/bgcol=333333/linkcol=ffffff/transparent=true/track=' + str.match(/(\d*).mp3$/)[1],
  remote = (append = []) => fetch("get_playlist.php?" + [ `q=${_qstr}`, `release=${_my.release}`, `label=${_my.label}`, ...append ].join('&')).then(response => response.json()),
  lookup = play => _db[play.path] ?
    new Promise(r => r(_db[play.path])) :
    fetch(`url2mp3.php?q=${_level}&path=${encodeURIComponent(play.path)}&u=${path_to_url(play.path)}`)
      .then(response => response.text())
      .then(data => {
        _db[play.path] = data;
        return data;
      });

function parsehash() {
  let hash = window.location.hash.slice(1).split('/');
  Object.assign(_my, {
    label: hash[0] || '',
    release: hash[1] || ''
  });
  _qstr = hash[3] || '';
  _level = hash[4] || 2;
  document.body.className = "q" + _level;
  return hash[2];
}

function play_url(play) {
  let src = path_to_url(play.path), ifr,
    fake = (_track.path === play.path),
    rel = _my.trackList, ttl = rel.length;
  
  if(!fake) {
    if(_level > 1) {
      console.log("Doing it!");
      ifr = _if ^= 1;
      _DOM[`if${ifr}`].className = 'in';

      // this type of iframe reassignmemt prevents the location history
      // from being populated so the back button still works.
      _DOM[`if${ifr}`].contentWindow.location.replace(src);

      // we delay the transition if it's the same album. 
      // Otherwise there's this temporary fading effect
      if(_track.release !== play.release) {
        _DOM[`if${+!ifr}`].className = 'out';
      }

      setTimeout(() => {
        if(_track.path === play.path) {
          _DOM[`if${+!ifr}`].className = 'out';
          _DOM[`if${+!ifr}`].contentWindow.location.replace(src);
        }
        _lock.hash = 0;
      }, 1000);
      _lock.hash = 1;
    }
    window.location.hash = [play.label, play.release, play.id, _qstr, _level].join('/');
  }
  ['release','label'].forEach(a => _DOM[a].innerHTML = _my[a].replace(/-/g, ' '))
  _DOM.track.innerHTML = `${play.id + 1}:${_my.trackList.length}<br/>${_my.number + 1}:${_my.count}`;

  _my.track = play.track;

  _next['+track'] = rel[(      play.id + 1) % ttl];
  _next['-track'] = rel[(ttl + play.id - 1) % ttl];
  _track = play;

  // This warms up the backend cache for the next tracks we can navigate to
  Object.values(_next).forEach(lookup);

  _DOM.controls.className = '';
  // this is the url to play.
  return fake? 
    new Promise(r => r()): 
    lookup(play).then(data => {
      // The file names can be really weird so we escape just that part of the path
      // Safari has issues with their PCRE so we are doing this dumber
      let parts = data.split('/'), fname = parts.pop();
      _DOM.player.src = [...parts, encodeURIComponent(fname)].join('/');

      //_DOM.player.src = data.replace(/(?:\/)([^\/]*)$/, a => encodeURIComponent(a))

      // being explicit like this seems to stop the media keys
      // from breaking
      _DOM.player.load();
      document.title = play.track;

      let [artist, title] = play.track.split(' - ');
      title = title ?? artist;
      _DOM.player.play();

      // There's a weird chrome bug here with doing another new operator.
      // I think these remediations are just voodoo ... I don't know what
      // the real bug is.
      if(navigator.mediaSession) {
        Object.assign( navigator.mediaSession.metadata, {
            title, artist,
            album: play.release
          },
          _level < 2 ? {} : {
            artwork: [96,128,192,256,384,512].map(r => { 
              return {
                src: play.path.replace(/\/[^\/]*$/,'') + `/album-art.jpg`, 
                sizes: `${r}x${r}`,
                type: 'image/jpeg'
              }
            })
        });
      }
   });
}

function d(skip, orig) {
  if(!_DOM.controls.className) {
    let next = _next[skip];

    if (next) { 
      if( !_lock.loop && (
            (skip == '+track'   && next.id === 0)
         || (skip == '-track'   && next.id >= _track.id)
         || (skip == '+release' && next.number == 0) 
         || (skip == '-release' && next.number >= _my.number)
        ) 
      ) {
        return d(skip[0] + (skip[1] === 't' ? 'release' : 'label'), orig || skip);
      }

      if('id' in next) {
        if(skip[1] === 't') {
          return play_url(next);
        } else if(!orig || skip === orig) {
          play_url(next);
        }
      }
    } 

    _DOM.controls.className = 'disabled';
    return remote([ `action=${skip}`, `orig=${orig || skip}` ])
      .then(data => {
        _my = data.release;
        delete data.release;
        _next = data;
        return play_url(_my.trackList[_my.track_ix]);
      });
  }
}

window.onload = () => {
  parsehash();

  'player if0 if1 label release top list nav navcontrols search track controls'.split(' ').forEach(
    what => _DOM[what] = document.querySelector(`#${what}`)
  );

  if (self.MediaMetadata) {
    navigator.mediaSession.metadata = new MediaMetadata();
    //
    // 1 tap  = track
    // 2 taps = release
    // 3 taps = label
    //
    [
      ['next','+'],
      ['previous', '-']
    ].forEach(([word, sign]) => 
      navigator.mediaSession.setActionHandler(`${word}track`, () => {
        _lock[sign] = (_lock[sign] || 0) + 1;
        if(!_lock[word]) {
          _lock[word] = setTimeout(() => {
            d( sign + ' track release label'.split(' ')[Math.min(_lock[sign], 3)] );
            _lock[sign] = _lock[word] = 0;
          }, 400);
        }
      })
    );
  }
  _DOM.track.onclick = function() {
    _lock.loop ^= 1;
    _DOM.track.className = (_lock.loop ? 'loop' : '');
  }

  _DOM.search.value = _qstr;

  _DOM.search.onkeydown = e => { 
    window.clearTimeout(_lock.search);
    _lock.search = window.setTimeout(() =>  {
      let newstr = encodeURIComponent(_DOM.search.value);
      if(newstr !== _qstr) {
        if (_DOM.search.value[0] == ':'){
          if (_DOM.search.value.length > 1) {
            _level = parseInt(_DOM.search.value[1], 10);
            _DOM.search.value = '';
            _db = {};
            document.body.className = "q" + _level;
          }
        } else {
          _qstr = newstr;
          _DOM.navcontrols.onclick();
        }
      }
    }, 250);
  }

  _DOM.navcontrols.onclick = e => {
    if (e) {
      let what = e.target;
      _tab = what.innerHTML;
      
      what.parentNode.childNodes.forEach(m => m.className = '');
      what.className = 'selected';
    }

    remote([ `action=${_tab}` ])
      .then(data => {
        _DOM.list.innerHTML = '';
        _DOM.list.append(...data.sort().map((obj,ix) => {
            let l = Object.assign(document.createElement('li'), {innerHTML: obj.track || obj, obj, ix});

            if(l.innerHTML === _my[_tab]){
              l.className = 'selected';
            }
            return l;
          })
        );
        // scroll to the element but only if the scrollbar is at the top.
        if(_DOM.list.scrollTop === 0 && _DOM.list.querySelector('.selected')) {
          _DOM.list.scrollTo(0, _DOM.list.querySelector('.selected').offsetTop - 150);
        }
      });
  }

  _DOM.list.onclick = e => {
    let ix = 0;
    if(e.target.tagName == 'LI'){
      if(_tab === 'track') {
        ix = e.target.ix;
        _my = e.target.obj;
      } else {
        _my[_tab] = e.target.innerHTML;
        if(_tab === 'label'){
          _my.release = '';
        }
      }
      d(ix).then(_DOM.navcontrols.onclick);
    }
  }

  document.body.onclick = e => {
    e = e.target;
    while(e != document.body){
      if (e === _DOM.search) {
        _DOM.nav.style.display = 'block';
        _DOM.navcontrols.onclick();
      }
      if(e === _DOM.top) {
        return;
      }
      e = e.parentNode;
    }
    _DOM.nav.style.display = 'none';
  };

  _DOM.player.onended = () => {
    d("+track");
    Notification.requestPermission().then(p => {
      if (p === "granted") {
        let s = decodeURIComponent(el.src).split('/').reverse();
        new Notification(s[1].replace(/-/g, ' ').toUpperCase(), {
          body: s[0].replace(/-(\d*).mp3$/,'')});
      }
    });
  }

  d(parsehash() ?? 0).then(_DOM.navcontrols.onclick);
  window.addEventListener('hashchange', () => !_lock.hash && d(parsehash()));
}
