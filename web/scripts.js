var _track = {},
  _my = {},
  _qstr,
  _next = {},
  _db = {},
  _tab = "track",
  _if,
  _play,
  // 0: opus
  // 1: heaac
  // 2: mp3
  _format = 2,
  // 0: recent
  // 1: all
  _filter = 1,
  _DOM = {},
  _lock = {},
  _start_at_start = false,
  path_to_url = (str) =>
    "https://bandcamp.com/EmbeddedPlayer/size=large/bgcol=333333/linkcol=ffffff/transparent=true/track=" +
    str.match(/(\d*).mp3$/)[1],
  remote = (append = []) =>
    fetch(
      "get_playlist.php?" +
      [
        `filter=${_filter}`,
        `level=${_format}`,
        `q=${_qstr}`,
        `release=${_my.release}`,
        `label=${_my.label}`,
        ...append,
      ].join("&"),
    ).then((response) => response.json()),
  lookup = (play) =>
    _db[play.path]
      ? new Promise((r) => r(_db[play.path]))
      : fetch(
        `url2mp3.php?q=${_format}&path=${encodeURIComponent(play.path)}&u=${path_to_url(play.path)}`,
      )
        .then((response) => response.text())
        .then((data) => {
          _db[play.path] = data;
          return data;
        });

function parsehash() {
  let hash = window.location.hash.slice(1).split("/");
  Object.assign(_my, {
    label: hash[0] || "",
    release: hash[1] || "",
  });
  _qstr = hash[3] || "";
  _format = hash[4] || 2;
  _filter = hash[5] || 1;
  document.body.className = "q" + _format;
  document.querySelectorAll('input[name="format"]')[_format].checked = true;
  document.querySelectorAll('input[name="filter"]')[_filter].checked = true;
  return hash[2];
}

function play_url(play) {
  if (!play) {
    window.location.hash = "";
    window.location.reload();
  }

  let src = path_to_url(play.path),
    ifr,
    fake = _track.path === play.path,
    rel = _my.trackList,
    ttl = rel.length;

  if (!fake) {
    if (_format > 1) {
      ifr = _if ^= 1;
      _DOM[`if${ifr}`].className = "in";

      // this type of iframe reassignmemt prevents the location history
      // from being populated so the back button still works.
      _DOM[`if${ifr}`].contentWindow.location.replace(src);

      // we delay the transition if it's the same album.
      // Otherwise there's this temporary fading effect
      if (_track.release !== play.release) {
        _DOM[`if${+!ifr}`].className = "out";
      }

      setTimeout(() => {
        if (_track.path === play.path) {
          _DOM[`if${+!ifr}`].className = "out";
          _DOM[`if${+!ifr}`].contentWindow.location.replace(src);
        }
        _lock.hash = 0;
      }, 1000);
      _lock.hash = 1;
    }
    window.location.hash = [
      play.label,
      play.release,
      play.id,
      _qstr,
      _format,
      _filter,
    ].join("/");
    _play = play;
  }
  ["release", "label"].forEach(
    (a) => (_DOM[a].innerHTML = _my[a].replace(/-/g, " ")),
  );
  _DOM.track.innerHTML = [
    "<div style=width:",
    (100 * (play.id + 1)) / _my.trackList.length,
    "%></div><div style=width:",
    (100 * (_my.number + 1)) / _my.count,
    "%></div>",
  ].join("");
  //_DOM.track.innerHTML = `${play.id + 1}:${_my.trackList.length}<br/>${_my.number + 1}:${_my.count}`;

  _my.track = play.track;

  _next["+track"] = rel[(play.id + 1) % ttl];
  _next["-track"] = rel[(ttl + play.id - 1) % ttl];
  _track = play;

  // This warms up the backend cache for the next tracks we can navigate to
  Object.values(_next).forEach(lookup);

  _DOM.controls.className = "";
  // this is the url to play.
  return fake
    ? new Promise((r) => r())
    : lookup(play).then((data) => {
      // The file names can be really weird so we escape just that part of the path
      // Safari has issues with their PCRE so we are doing this dumber
      let parts = data.split("/"),
        fname = parts.pop();
      _DOM.player.src = [...parts, encodeURIComponent(fname)].join("/");

      //_DOM.player.src = data.replace(/(?:\/)([^\/]*)$/, a => encodeURIComponent(a))

      // being explicit like this seems to stop the media keys
      // from breaking
      _DOM.player.load();
      document.title = play.track;

      let [artist, title] = play.track.split(" - ");
      title = title ?? artist;
      //_DOM.player.play();

      // There's a weird chrome bug here with doing another new operator.
      // I think these remediations are just voodoo ... I don't know what
      // the real bug is.
      if (navigator.mediaSession) {
        Object.assign(
          navigator.mediaSession.metadata,
          {
            title,
            artist,
            album: play.release,
          },
          _format < 2
            ? {}
            : {
              artwork: [96, 128, 192, 256, 384, 512].map((r) => {
                return {
                  src:
                    play.path.replace(/\/[^\/]*$/, "") + `/album-art.jpg`,
                  sizes: `${r}x${r}`,
                  type: "image/jpeg",
                };
              }),
            },
        );
      }
    });
}

function bail (){
  // reset the failpoints.
  parts = window.location.hash.split('/');
  // track offset
  parts[2] = 0;
  // search term
  parts[3] = '';
  // quality
  parts[4] = 2;
  // recent
  parts[5] = 1;
  window.location.hash = parts.join('/');
  location.reload();
}
function d(skip, orig) {
  if (!_DOM.controls.className) {
    let next = _next[skip];

    if (next) {
      if (
        !_lock.loop &&
        ((skip == "+track" && next.id === 0) ||
          (skip == "-track" && next.id >= _track.id) ||
          (skip == "+release" && next.number == 0) ||
          (skip == "-release" && next.number >= _my.number))
      ) {
        return d(
          skip[0] + (skip[1] === "t" ? "release" : "label"),
          orig || skip,
        );
      }

      if ("id" in next) {
        if (skip[1] === "t") {
          return play_url(next);
        } else if (!orig || skip === orig) {
          play_url(next);
        }
      }
    }

    if (_format > 1) {
      _DOM.controls.className = "disabled";
    }
    return remote([`action=${skip}`, `orig=${orig || skip}`]).then((data) => {
      if (_my) {
        _my = data.release;
        delete data.release;
        _next = data;
        if(!_my.trackList || _my.track_ix == -1) {
          bail();
        } else {
          return play_url(_my.trackList[_my.track_ix]);
        }
      }
    });
  }
}

function setLevel(what) {
  _format = what;
  _DOM.search.value = "";
  _db = {};
  document.body.className = "q" + _format;
}

function voiceSearch() {
  const recognition = new (window.SpeechRecognition ||
    window.webkitSpeechRecognition)();
  recognition.continuous = true;
  recognition.interimResults = true;

  recognition.onresult = function(event) {
    const transcript = event.results[event.resultIndex][0].transcript;
    _qstr = transcript;
    _DOM.navcontrols.onclick();
    Object.assign(navigator.mediaSession.metadata, {
      title: _qstr,
      artist: "Voice Search",
    });
  };

  recognition.onerror = function(event) {
    console.error("Speech recognition error: ", event.error);
  };
  Object.assign(navigator.mediaSession.metadata, {
    title: "Voice Search",
  });

  recognition.start();
}

window.onload = () => {
  parsehash();

  "prefs start player if0 if1 label release top list nav navcontrols search track controls"
    .split(" ")
    .forEach((what) => (_DOM[what] = document.getElementById(what)));

  if (self.MediaMetadata) {
    let pauseFlag = false,
      toggleTime;
    navigator.mediaSession.metadata = new MediaMetadata();

    navigator.mediaSession.setActionHandler("pause", () => {
      _DOM.player.pause();
      Object.assign(navigator.mediaSession.metadata, {
        title: "Paused",
      });
      pauseFlag = true;
    });
    navigator.mediaSession.setActionHandler("play", async () => {
      let delta = new Date() - toggleTime;
      if (delta < 500) {
        return;
      }
      await _DOM.player.play();

      Object.assign(navigator.mediaSession.metadata, {
        title: "Play",
        artist: delta,
      });
      pauseFlag = false;
    });
    //
    // 1 tap  = track
    // 2 taps = release
    // 3 taps = label
    //
    [
      ["next", "+"],
      ["previous", "-"],
    ].forEach(([word, sign]) =>
      navigator.mediaSession.setActionHandler(`${word}track`, () => {
        if (pauseFlag) {
          voiceSearch();
          toggleTime = new Date();
          return true;
        }
        _lock[sign] = (_lock[sign] || 0) + 1;
        if (!_lock[word]) {
          _lock[word] = setTimeout(() => {
            d(
              sign +
              " track release label".split(" ")[Math.min(_lock[sign], 3)],
            );
            _lock[sign] = _lock[word] = 0;
          }, 400);
        }
      }),
    );
  }
  _DOM.track.onclick = function() {
    _lock.loop ^= 1;
    _DOM.track.className = _lock.loop ? "loop" : "";
  };

  _DOM.search.value = _qstr;

  _DOM.search.onkeydown = (e) => {
    window.clearTimeout(_lock.search);
    _lock.search = window.setTimeout(() => {
      let newstr = encodeURIComponent(_DOM.search.value);
      if (newstr !== _qstr) {
        _qstr = newstr;
        _DOM.navcontrols.onclick();
        _lock.hash = 1;

        if (_play) {
          window.location.hash = [
            _play.label,
            _play.release,
            _play.id,
            _qstr,
            _format,
            _filter,
          ].join("/");
        }
        window.setTimeout(() => {
          _lock.hash = 0;
        }, 10000);
      }
    }, 250);
  };

  _DOM.navcontrols.onclick = (e) => {
    if (e) {
      let what = e.target;
      _tab = what.innerHTML;

      what.parentNode.childNodes.forEach((m) => (m.className = ""));
      what.className = "selected";
    }
    if (_tab == "prefs") {
      _DOM.list.innerHTML = "";
      _DOM.list.appendChild(_DOM.prefs);
      return;
    }

    remote([`action=${_tab}`]).then((data) => {
      try {
        _DOM.list.removeChild(_DOM.list.firstElementChild);
      } catch (e) { }
      _DOM.list.innerHTML = "";
      _DOM.list.append(
        ...data.sort().map((obj, ix) => {
          let l = Object.assign(document.createElement("li"), {
            innerHTML: obj.track || obj.release || obj,
            obj,
            ix,
          });

          if (l.innerHTML === _my[_tab]) {
            l.className = "selected";
          }
          return l;
        }),
      );
      // scroll to the element but only if the scrollbar is at the top.
      if (_DOM.list.scrollTop === 0 && _DOM.list.querySelector(".selected")) {
        _DOM.list.scrollTo(
          0,
          _DOM.list.querySelector(".selected").offsetTop - 150,
        );
      }
    });
  };

  _DOM.list.onclick = (e) => {
    let ix = 0;
    if (e.target.tagName == "INPUT") {
      if (e.target.name == "format") {
        setLevel(+e.target.value);
      }
      if (e.target.name == "filter") {
        _filter = +e.target.value;
      }
    } else if (e.target.tagName == "LI") {
      if (_tab === "track" || _tab === "release") {
        ix = e.target.obj.track_ix;
        _my = e.target.obj;
      } else {
        _my[_tab] = e.target.innerHTML;
        if (_tab === "label") {
          _my.release = "";
        }
      }
      console.log(_my);
      d(ix).then(_DOM.navcontrols.onclick);
    }
  };

  document.body.onclick = (e) => {
    e = e.target;
    while (e != document.body) {
      if (e === _DOM.search) {
        _DOM.nav.style.display = "block";
        _DOM.navcontrols.onclick();
      }
      if (e === _DOM.top) {
        return;
      }
      e = e.parentNode;
    }
    _DOM.nav.style.display = "none";
  };

  _DOM.player.addEventListener("durationchange", (e) => {
    // this is a one off ignoring of the start
    if (_start_at_start) {
      _start_at_start = false;
    } else {
      e.target.currentTime = (_DOM.start.value / 100) * e.target.duration;
    }
    _DOM.player.play();
  });

  _DOM.player.onended = () => {
    // we want to differentiate this track forward from the tapped one
    // in order to go to the beginning of the track
    _start_at_start = true;
    d("+track");
    if (_format > 1) {
      Notification.requestPermission().then((p) => {
        if (p === "granted") {
          let s = decodeURIComponent(_DOM.player.src).split("/").reverse();
          new Notification(s[1].replace(/-/g, " ").toUpperCase(), {
            body: s[0].replace(/-(\d*).mp3$/, ""),
          });
        }
      });
    }
  };

  try {
    d(parsehash() ?? 0).then(_DOM.navcontrols.onclick);
  } catch(ex) {
    bail();
  }
  window.addEventListener("hashchange", () => !_lock.hash && d(parsehash()));
};
