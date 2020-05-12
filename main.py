import json
import logging
import sqlite3
import time
from datetime import datetime

import praw
import spotipy
from furl import furl
from spotipy.oauth2 import SpotifyClientCredentials


def main():
    # Build the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s", "%Y/%m/%d %H:%M:%S")

    # File handler
    file_handler = logging.FileHandler("SpotifyInfoBot.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Stream handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Open and create the database if needed
    sql = sqlite3.connect("data.db")
    cur = sql.cursor()

    # Create our connection to Reddit
    with open("config.json", "r", encoding="UTF-8") as config_file:
        config = json.load(config_file)

    reddit = praw.Reddit(
        username="SpotifyInfoBot",
        password=config["reddit"]["password"],
        client_id=config["reddit"]["client_id"],
        client_secret=config["reddit"]["client_secret"],
        user_agent="SpotifyInfoBot:v1.0 (by /u/Golden_Narwhal)",
    )

    logger.info("Successfully authenticated as %s", reddit.user.me())

    # Create out connection to Spotify
    spotify_credentials_manager = SpotifyClientCredentials(
        client_id=config["spotify"]["client_id"], client_secret=config["spotify"]["client_secret"],
    )
    spotify = spotipy.Spotify(client_credentials_manager=spotify_credentials_manager)

    subreddit = reddit.subreddit("SpotifyPlaylists")

    # Cache of submission IDs to avoid responding to
    submission_ids = []

    # Work loop
    while True:
        # Get the newest 100 submissions
        try:
            new_submissions = subreddit.new(limit=100)
        except Exception as err:
            logger.info("Failed to fetch new submissions")

            retry = True
            sleep_time = 2
            attempt_count = 1

            while retry:
                logger.info("Reattempting fetch in %d seconds", sleep_time)

                time.sleep(sleep_time)

                if sleep_time < 32:
                    sleep_time *= 2

                try:
                    new_submissions = subreddit.new(limit=100)

                    retry = False
                except Exception as err:
                    logger.info("Reattempt #%d failed", attempt_count)
                    attempt_count += 1

        for submission in new_submissions:
            if submission.id in submission_ids:
                continue

            # Ignore submissions that we missed, but are older than 15 minutes
            if int(submission.created_utc) < (int(datetime.utcnow().timestamp()) - 900):
                submission_ids.append(submission.id)
                continue

            cur.execute("select * from submissions where id=?", [submission.id])
            if cur.fetchone():
                submission_ids.append(submission.id)
                continue

            # Parse the URL
            f = furl(submission.url)

            # TODO: Is this the only host we need to care about?
            if f.host != "open.spotify.com":
                logger.info("Invalid Spotify URL: {}".format(submission.url))
                submission_ids.append(submission.id)
                continue

            # Get the playlist ID from the URL
            playlist_id = ""
            if f.path.segments[0].lower() == "playlist":
                playlist_id = f.path.segments[1]
            elif f.path.segments[0].lower() == "user":
                playlist_id = f.path.segments[3]
            else:
                logger.info("Couldn't find a playlist ID, ignoring")
                submission_ids.append(submission.id)
                continue

            logger.info("New submission: https://www.reddit.com{}".format(submission.permalink))

            # Get the playlist and all of it's tracks
            try:
                playlist = spotify.playlist(playlist_id)
            except spotipy.exceptions.SpotifyException as err:
                # TODO: I'm pretty sure this will only catch if the playlist ID is invalid
                submission_ids.append(submission.id)
                continue
            except Exception as err:
                logger.info("Failed to fetch playlist")

                retry = True
                sleep_time = 2
                attempt_count = 1

                while retry:
                    logger.info("Reattempting fetch in %d seconds", sleep_time)

                    time.sleep(sleep_time)

                    if sleep_time < 32:
                        sleep_time *= 2

                    try:
                        playlist = spotify.playlist(playlist_id)

                        retry = False
                    except Exception as err:
                        logger.info("Reattempt #%d failed", attempt_count)
                        attempt_count += 1

            offset = 100
            while len(playlist["tracks"]["items"]) < playlist["tracks"]["total"]:
                try:
                    tracks = spotify.playlist_tracks(playlist_id, offset=offset)
                except Exception as err:
                    # If we made it this far, then the playlist ID has to be valid, so this can only be a network issue
                    logger.info("Failed to fetch playlist tracks")

                    retry = True
                    sleep_time = 2
                    attempt_count = 1

                    while retry:
                        logger.info("Reattempting fetch in %d seconds", sleep_time)

                        time.sleep(sleep_time)

                        if sleep_time < 32:
                            sleep_time *= 2

                        try:
                            tracks = spotify.playlist_tracks(playlist_id, offset=offset)

                            retry = False
                        except Exception as err:
                            logger.info("Reattempt #%d failed", attempt_count)
                            attempt_count += 1

                playlist["tracks"]["items"].extend(tracks["items"])
                offset += 100

                time.sleep(1)

            # Calculate the length of the playlist
            total_ms = sum(
                track["track"]["duration_ms"] for track in playlist["tracks"]["items"] if track["track"] is not None
            )

            hours = int(total_ms / 3600000)
            minutes = int((total_ms / 60000) % 60)

            # Rank the tracks by popularity
            ranked_tracks = sorted(
                playlist["tracks"]["items"],
                key=lambda track: track["track"]["popularity"] if track["track"] is not None else 0,
                reverse=True,
            )

            # Build the response
            response = "Playlist name: [{}]({})\n\n".format(playlist["name"], submission.url)
            response += "Playlist author: [{}]({})\n\n".format(
                playlist["owner"]["display_name"], playlist["owner"]["external_urls"]["spotify"],
            )
            response += "Number of tracks: {}\n\n".format(len(playlist["tracks"]["items"]))
            response += "Length: {} hr {} min\n\n".format(hours, minutes)
            response += "Followers: {:,}\n\n".format(playlist["followers"]["total"])
            response += "Top tracks:\n\n"

            for index, track in enumerate(ranked_tracks[:5]):
                artists_raw = [artist["name"] for artist in track["track"]["artists"]]

                if len(artists_raw) == 1:
                    arists_string = artists_raw[0]
                elif len(artists_raw) == 2:
                    arists_string = "{} and {}".format(artists_raw[0], artists_raw[1])
                else:
                    arists_string = ", ".join(artists_raw[:-2] + [", and ".join(artists_raw[-2:])])

                response += "* [{} - {}]({})\n".format(
                    arists_string, track["track"]["name"], track["track"]["external_urls"]["spotify"]
                )

            try:
                submission.reply(response)
            except Exception as err:
                # TODO: I'm sure there's other reasons this could catch, but for now I'm assuming it's a network issue
                logger.info("Failed to submit response")

                retry = True
                sleep_time = 2
                attempt_count = 1

                while retry:
                    logger.info("Reattempting submission in %d seconds", sleep_time)

                    time.sleep(sleep_time)

                    if sleep_time < 32:
                        sleep_time *= 2

                    try:
                        submission.reply(response)

                        retry = False
                    except Exception as err:
                        logger.info("Reattempt #%d failed", attempt_count)
                        attempt_count += 1

            submission_ids.append(submission.id)

            cur.execute(
                "insert into submissions values(?,?)", [submission.id, int(datetime.utcnow().timestamp())],
            )
            sql.commit()

            while len(submission_ids) > 125:
                submission_ids.pop(0)

            time.sleep(1)


if __name__ == "__main__":
    main()
