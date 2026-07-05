import json
import time
import uuid
from pathlib import Path

import psycopg2
import streamlit as st

from utils import ccloud_lib

# Database connection details
HOST = "fhtw-big-data.postgres.database.azure.com"
DATABASE = "music_store"
USER = "student"
PASSWORD = "reRZ2pjg1WxqlwjU"

# Kafka connection details
CONFIG_FILE = Path(__file__).with_name("kafka.config")
GROUP_NAME = "group_ghanem-bilalic"
KAFKA_TOPIC = f"{GROUP_NAME}-music"  # ONE topic for this group, created on first run if missing

# Where the Spark job (see assignment.ipynb) writes recommendations for each user.
# Both this app and the notebook read/write this file from the same "assignment" folder.
RECOMMENDATIONS_FILE = Path(__file__).with_name("recommendations.json")
RECOMMENDATION_THRESHOLD = 10  # must match the threshold used in assignment.ipynb

ACTIONS = {
    "like": ("👍 Like", "You liked the track!"),
    "dislike": ("👎 Dislike", "You disliked the track!"),
    "skip": ("⏭️ Skip", "Skipped to the next track."),
}


# --- Database ---------------------------------------------------------------

@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=HOST,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
    )
    return conn


def get_random_track(conn):
    """Fetch a random track together with the metadata we stream to Kafka."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.name, a.name, al.title, g.name, t.unit_price
            FROM public.tracks t
            JOIN public.albums al ON t.album_id = al.id
            JOIN public.artists a ON al.artist_id = a.id
            JOIN public.genres g ON t.genre_id = g.id
            ORDER BY random()
            LIMIT 1;
            """
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "track_id": row[0],
        "track_name": row[1],
        "artist": row[2],
        "album": row[3],
        "genre": row[4],
        "unit_price": float(row[5]),
    }


# --- Kafka -------------------------------------------------------------------

@st.cache_resource
def get_kafka_config():
    config = ccloud_lib.read_ccloud_config(str(CONFIG_FILE))
    # Make sure our topic exists before the first event is produced.
    ccloud_lib.create_topic(config, KAFKA_TOPIC)
    return config


@st.cache_resource
def get_kafka_producer():
    from confluent_kafka import Producer

    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(
        get_kafka_config().copy()
    )
    return Producer(producer_conf)


def send_event(user_id, action, track):
    """Build one user-interaction event and produce it to KAFKA_TOPIC."""
    event = {
        "user_id": user_id,
        "track_id": track["track_id"],
        "action": action,  # like | dislike | skip
        "ts": time.time(),
        "track_name": track["track_name"],
        "artist": track["artist"],
        "album": track["album"],
        "genre": track["genre"],
        "unit_price": track["unit_price"],
    }
    producer = get_kafka_producer()
    producer.produce(KAFKA_TOPIC, key=user_id, value=json.dumps(event))
    producer.poll(0)
    producer.flush()


# --- Recommendations (written by the Spark job in assignment.ipynb) --------

def load_recommendations_for_user(user_id):
    if not RECOMMENDATIONS_FILE.exists():
        return None
    try:
        with open(RECOMMENDATIONS_FILE) as f:
            all_recs = json.load(f)
    except json.JSONDecodeError:
        return None
    return all_recs.get(user_id)


# --- Streamlit UI -------------------------------------------------------------

def main():
    st.set_page_config(page_title="Track Recommender", page_icon="🎵")
    conn = get_connection()

    # Persistent per-browser-session user id, used as the Kafka event key
    # and as the lookup key into recommendations.json.
    if "user_id" not in st.session_state:
        st.session_state.user_id = f"user-{uuid.uuid4().hex[:8]}"
    if "interaction_count" not in st.session_state:
        st.session_state.interaction_count = 0
    if "current_track" not in st.session_state:
        st.session_state.current_track = get_random_track(conn)

    with st.sidebar:
        st.caption(f"Group: {GROUP_NAME}  ·  Topic: {KAFKA_TOPIC}")
        st.subheader("Session")
        st.text_input("Your user ID", key="user_id_input", value=st.session_state.user_id)
        if st.session_state.user_id_input != st.session_state.user_id:
            st.session_state.user_id = st.session_state.user_id_input
        st.caption(
            "Change this to simulate a different listener. "
            "Every Like/Dislike/Skip is streamed to Kafka as one event."
        )
        st.metric("Interactions logged this session", st.session_state.interaction_count)

    st.title("🎵 Track Recommender")

    track = st.session_state.current_track
    if track:
        st.header(track["track_name"])
        st.subheader(f"Artist: {track['artist']}")
        st.caption(
            f"Album: {track['album']} · Genre: {track['genre']} · Price: ${track['unit_price']:.2f}"
        )

        cols = st.columns(len(ACTIONS))
        for col, (action, (label, message)) in zip(cols, ACTIONS.items()):
            if col.button(label, use_container_width=True):
                send_event(st.session_state.user_id, action, track)
                st.session_state.interaction_count += 1
                if action == "like":
                    st.success(message)
                elif action == "dislike":
                    st.error(message)
                else:
                    st.info(message)
                # Move on to a new track after every action.
                st.session_state.current_track = get_random_track(conn)
                st.rerun()
    else:
        st.warning("No track found in the database.")

    st.divider()
    st.subheader("Your recommendations")
    st.caption(
        "Generated by the Spark Structured Streaming job in assignment.ipynb once you "
        f"have logged at least {RECOMMENDATION_THRESHOLD} interactions. Keep that notebook's "
        "streaming cells running while you use this app."
    )

    user_recs = load_recommendations_for_user(st.session_state.user_id)
    if user_recs:
        st.write(
            f"Based on {user_recs['interactions']} interactions "
            f"(last updated {time.strftime('%H:%M:%S', time.localtime(user_recs['generated_at']))})"
        )
        for rec in user_recs["tracks"]:
            st.write(
                f"**{rec['rank']}. {rec['track_name']}** — {rec['artist']} "
                f"· {rec['genre']} · ${rec['unit_price']:.2f} (score {rec['score']})"
            )
    else:
        remaining = max(0, RECOMMENDATION_THRESHOLD - st.session_state.interaction_count)
        if remaining:
            st.info(f"Log {remaining} more interaction(s) to unlock recommendations.")
        else:
            st.info(
                "Threshold reached - waiting for the Spark job to process your events "
                "and write recommendations.json. Wait a few seconds and then click on any button (Like, Dislike, Skip)."
            )


if __name__ == "__main__":
    main()