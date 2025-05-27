# Mutiny: Intentionally Underground Music Explorer

Services like spotify don't cut it. They end up tending towards some definition of collective approval, some kind of popular.

Other systems try to classify music and look for similar orchestration and structure. Wrong again. that's not what new music is about.

The random walk of discovery systems such as youtube music recommendation would eventually tend towards mainstream indy or pop artists and then form a tightly clustered and repetitive network around them. 

All these systems suck and it's one of the primary reasons pop music has stagnated since their rise,

So I wanted a tool for exploring a mix of variability, consistency and novelty that expanded my horizons without these vanishing gradient properties.

This is my fourth attempt, since 2006 or so, of building that tool, this one starting in 2020.

## Overview

Essentially the pipeline, in non-technical terms is:

 * automate the collection of stuff to listen to with a curation system flexible enough to include serendipity but focused enough to avoid exhaustion
 * have a system of navigating and listening to it that can be instrumented in a variety of ways
 * record in a simple manner what to do with it: what I'd like to revisit, go off and buy, keep around, or is not for me.

So this more or less does that.

There's a secret part, which I may you know, try to get some kind of money for, and an open part. 

---
The rest of this document is from an older readme about the project. This is a work in progress and I'm trying to make it more comprehensible. Sorry for the disorganization.
---

A set of tools for exploring music surrounding yt-dlp and mpv (although that is configurable)

There's also a way to navigate and control the music that uses lua and optionally things like usb foot pedals or MIDI controllers, really. I also use an arduino with an LCD display for the tracks, that  code is in there too. These are all optional and the tools work seamlessly with or without them, whether you are connected or not connected to the internet.

Additionally there's music-discovery navigation tools that involve discogs, search engine apis, soundcloud, archive.org, youtube, and bandcamp. That one is done in python and has a web-interface (the code isn't here yet). Hopefully I'll get it cleaned up and presentable.

There is also a web player currently, whose code is included, here's one against my collection [https://mutiny.website](https://mutiny.website). 

It works by ... essentially cloning entire labels. I know what you're saying "that sounds shady." --- I've actually spent 300% more buying music on bandcamp month over month than I did before because this tool exposes artists to me for me to buy.

The abstractions are like any powerful sets of tools: your personal moral compass is the guide to how you use it.

## Getting music

This tool is label/artist based - you'll need an *unknown number* of gigabytes of disk space free (depending on your apetite) - set aside 250GB or so, disks are cheap, under $10/TB, stop raising your eyebrows at me.

The first thing to do is run `./install.sh` using a resolvable path as the arg. (`$HOME/bin` is the default)

    $ ./install.sh

Ok, that was easy. Now figure out where you want the music to go. I'm going to use `/sd/mp3/label`

    $ mkdir -p /sd/mp3/label
    $ cd /sd/mp3/label

Let's say you want to get a label or artist, heck let's use me, pay me nothing, it's cool.

    $ album-get chrismckenzie
    ♫ chrismckenzie ♫
    --- /sd/mp3/label/chrismckenzie/astrophilosophy
      ⇩ https://chrismckenzie.bandcamp.com/album/astrophilosophy
    ...
    $

Woah shit, what just happened? 

(this is the download version) 

    $ tree chrismckenzie
    chrismckenzie
    ├── astrophilosophy
    │   ├── chris mckenzie - Astrophilosophy-3196176877.mp3
    │   ├── chris mckenzie - Instrumentals-2161707097.mp3
    │   ├── chris mckenzie - Vocals-589691184.mp3
    │   ├── domain
    │   ├── exit-code
    │   ├── page.html
    │   └── playlist.m3u
    └── textures-i
        ├── chris mckenzie - 6AM-3099860585.mp3
        ├── chris mckenzie - Dawn Break-3496240403.mp3
        ├── chris mckenzie - Drunken Princess-1900005411.mp3
        ├── chris mckenzie - Homage to Vangelis-515942329.mp3
        ├── chris mckenzie - October Wind-2524682866.mp3
        ├── chris mckenzie - Rose-2272020348.mp3
        ├── chris mckenzie - Space Royalty-3306453676.mp3
        ├── domain
        ├── exit-code
        └── playlist.m3u

That's all my stuff along with `yt-dlp`'s exit codes that get checked for errors. (this is configurable)
The `page.html` is a cached version of the page as the scraper saw it at the time.

Here's another version you can have which just downloads links that get updated to the tracks.

    $ tree chrismckenzie
    chrismckenzie
    ├── astrophilosophy
    │   ├── domain
    │   ├── url-list.m3u
    │   └── exit-code
    └── textures-i
        ├── domain
        ├── url-list.m3u
        └── exit-code

The url-list.m3u is an m3u that gets created through bash and awk (see ytdl2m3u.awk) that will eventually get fed back into mpv.

The domain is because some albums are hosted on different sites. Here's an example of where that can be useful. Under the [rohsrecords](https://rohsrecords.bandcamp.com) label there's an artist whose content is [mforsleep](https://mforsleep.bandcamp.com/) so if you pull rohsrecords, you can filter for mforsleep like so:

    $ grep -l mforsleep rohsrecords/*/domain | cut -d '/' -f 1,2

After a mature crawl and some cron-job work, this is my current tree as of Apr 2024, 4 years into the project: [https://9ol.es/tmp/tree.html](https://9ol.es/tmp/tree.html) (warning: 490,000 lines and 25MB of html)

#### But wait, there's more!

So let's say you want to see if I added anything, just run it again.

    $ album-get chrismckenzie
    ♫ chrismckenzie ♫
    $

It's oh so clever and sees there's nothing new.  

Alright, now let's say you do this for a bunch of other labels and artists. Let's add a second one, oh I dunno, say cpurecords.

CPU records uses its own custom domain, https://shop.cpurecords.net/ but it is in fact, just a bandcamp site. 

To get it we will use the same tool. It's once again, pretty smart.

The syntax however is a little different: we specify the url, followed by the name:

    $ album-get shop.cpurecords.net cpurecords
    ♫ cpurecords ♫
    ... chug chug chug ...
    $

Now you can see a file `cpurecords/domain` which has the real domain. This is important for the next part.

#### Mass updating

So you go along and have say 20 labels you're browsing through, making it rain on a bunch of amateur musicians, and a week passes. You want to see what's new.  We use our clever command, in the directory, but this time with no arguments.  It will try to pull new stuff from everyone.

    $ album-get
    ♫ chrismckenzie ♫
    ♫ cpurecords ♫
    ... chug chug chug ...
    $

This is effectively equivalent to the social media concept of "following" and "feed" albiet a rather cobbled together inefficient orchestration.

## Playing music

Now you have all of this stuff to go through you *could just do it in a disorganized manner* maybe loading it into some gui tool and then trying to sort through it. 

**No! We are better than that**

Instead what we are going to do is play each release ONCE, then decide what to do with it. The ones you like, feel free to do what is right and go and buy things, I do.

Here's how we do it

    $ mutiny

    https://chrismckenzie.bandcamp.com/album/astrophilosophy

    Playing: chrismckenzie/astrophilosophy/chris mckenzie - Astrophilosophy-3196176877.mp3
    .. listen

    Exiting... (Quit)
    [chrismckenzie/astrophilosophy]


"Oh great, a REPL". you say

Don't worry, it'll be easy.

Here we decide what to do with what we just heard. We can

  * r - replay it
  * p - purge (move it /tmp and mark it as undesired)
  * s - skip the decision making
  * dl - download it
  * 3-5 - rate it from 3-5
  * q - exit

Actually there's documentation with the "?", see

![documentation](https://github.com/kristopolous/music-explorer/assets/231761/e88a846e-306b-4a0b-b458-faec2c4c2e40)

That wasn't painful, hopefully. There's quite a bit more just use ? to see the various commands.

I'm going to decide to dump my own music, that slouch is awful.

    [chrismckenzie/textures-i] p
    + base=/sd/mp3/label
    + mkdir -p /tmp/chrismckenzie/astrophilosophy
    + mv '/sd/mp3/label/chrismckenzie/astrophilosophy/chris mckenzie - Astrophilosophy-3196176877.mp3' '/sd/mp3/label/chrismckenzie/astrophilosophy/chris mckenzie - Instrumentals-2161707097.mp3' '/sd/mp3/label/chrismckenzie/astrophilosophy/chris mckenzie - Vocals-589691184.mp3' /sd/mp3/label/chrismckenzie/astrophilosophy/exit-code /tmp/chrismckenzie/astrophilosophy
    + touch /sd/mp3/label/chrismckenzie/astrophilosophy/no

And there we go. A placeholder file is put there so that when album-get comes through again, it won't try to grab it again.

After you exit this tool, the diligent students will notice a few dot files have been created:

    $ ls -1 .*
    .dl_history
    .listen_all
    .listen_done

Here's the one you want to look at (The other two are just for management/overhead)

### .listen_done - the list of releases you've gone through.

The format is 

    path __rating__ date

This is so you can do something like:

    $ awk ' { print $NF } ' .listen_done | sort | uniq -c

And see how many you go through every day. Kinda interesting. 

You can also do this:

    $ grep rating_5 .listen_done

And see all the stuff you gave a high rating to.

### Making it work without the infrastructure

So even in a readonly mount of directories this system can work. Here's an example:

    $ NOSCORE=1 NOPL=1 mutiny

This says you aren't trying to label (score) the system and you don't care about playlists. This is nice if you're like me and have this player integrated
into your window manager but you're traveling and just want music to play. Then I have all the infrastructure with multi-sinks, dynamic sourcing, input control
support etc, without having to worry about any of the dot files or the meta information.

A bunch of things won't work such as some of the mpv key bindings from the lua script but that's fine.

### Low Bandwidth support

The repl supports on-demand converting to HE AAC+ and Opus for instances where you are doing say, ssh remote mounting. So let's pretend you're doing this:

     $ sshfs -o cache=yes -o kernel_cache -o Ciphers=aes192-ctr -v -p 4021 example.com:/raid raid
     $ cd raid/label
     $ mutiny

Normally you'll get the mp3 sent over the wire for you to consume. But what if you're metered? Try this

     $ REMOTE=example.com REMOTEBASE=/raid/label FMT=opus mutiny

Now before playing the script will ssh to the remote machine and transcode everything to a lower bitrate so the files that go over the wire are smaller.
The format is as follows:

     Opus -     15000 b/s
     HE AAC+ -  32000 b/s done via fdkaac (HE-AAC v2 (SBR+PS))  

They don't sound *that awful* and you get at least a 75% drop in bitrate.

This system is also nice because it keeps the transcoder and storage point independent. fdkaac seems to do suspiciously better on Intel hardware over AMD (like 8x more than you expect) so farming that out to say a 1x-gen i7 isn't a bad idea.

-----

The web interface, which is really not documented at all also has this if you search ":0" ":1" and ":2" for opus/heaac/mp3 accordingly. iOS Safari doesn't support
opus or heaac but linux and android have no problems with it (they were picked because HTML5 audio has support). It *should* fallback to a less effecient format if
the better one doesn't exist. Unlike with mutiny you'll have to convert these on your own.

Something like 

```bash
$ sqlite3 playlist.db "select path from tracks" | while read p; do 
    echo $p
    mutlib toopus $p
    mutlib tom5a $p
  done
```

Note the "m**5**a" fake extension here to not collide with any potentially existing m4a files.

What's that mutlib thing? Well that's documented as well:

![help](https://github.com/kristopolous/music-explorer/assets/231761/1f7616f4-66ef-4ae6-8265-644c45631104)

and you can also inspect the function for instance:

![opus](https://github.com/kristopolous/music-explorer/assets/231761/ad30baf8-251b-4823-b2c9-88140369484f)



