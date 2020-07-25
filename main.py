import os
import asyncio
import argparse
import logging
from enum import Enum
from itertools import chain
import requests
from tqdm import tqdm


parser = argparse.ArgumentParser(
    prog="nuke",
    description="A program to delete all your discord messages",
    epilog="Author: github.com/ratsclub <freire.victor@tuta.io>")

parser.add_argument("--log", metavar="FILE", type=str,
                    default="nuke.log",
                    help="location to save log file (default: ./nuke.log)")
parser.add_argument("--token", metavar="TOKEN", type=str,
                    help="your discord account token")

args = parser.parse_args()

# logging
logger = logging.getLogger("nuke")
logger.setLevel(logging.DEBUG)

# handlers
file_handler = logging.FileHandler(filename=args.log)
file_handler.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(stream=None)
stream_handler.setLevel(logging.ERROR)
# formatter
formatter = logging.Formatter(
    "[%(levelname)s] %(funcName)s, %(asctime)s:  %(message)s",
    "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

BASE_URL = "https://discord.com/api"
ME_USERS_URL = f"{BASE_URL}/users/@me"
ME_USERS_GUILDS_URL = f"{BASE_URL}/users/@me/guilds"
ME_USERS_CHANNELS_URL = f"{BASE_URL}/users/@me/channels"


class ChatKind(Enum):
    Channel = "channels"
    Guild = "guilds"


async def search_messages(
        session: requests.Session,
        chat: dict,
        user: dict,
        params: dict = {},
):
    chat_id = chat["id"]
    chat_kind = chat["kind"].value
    user_id = user["id"]

    log_str = f"| {chat_kind[:-1]} {chat_id} | params {params} |"
    logger.info(log_str)

    params = {**params, "author_id": user_id}
    path = f"{chat_kind}/{chat_id}/messages/search"

    while True:
        resp = session.get(
            f"{BASE_URL}/{path}",
            params=params
        )

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
            logger.info(f"rate limit {retry_after}ms {log_str}")
            await asyncio.sleep(retry_after / 1000)
            continue

        return resp.json()


async def search_messages_worker(
        session: requests.Session,
        chat: dict,
        user: dict,
        queue: asyncio.Queue,
        params: dict = {},
):
    params = {}
    chat_kind = chat["kind"].value
    chat_id = chat["id"]

    while True:
        result = await search_messages(session, chat, user, params)
        total = result["total_results"]
        messages = result["messages"]

        # no messages left
        if total == 0:
            logger.info(f"done on {chat_kind} {chat_id}!")
            return

        # getting my own messages
        messages = [msg for msg in chain(*messages) if "hit" in msg]

        # sending messages to queue
        for msg in messages:
            await queue.put({
                "id": msg["id"],
                "channel": msg["channel_id"],
                "kind": chat_kind
            })

        # getting the oldest id
        ids = [msg["id"] for msg in messages]
        max_id = min(sorted(ids, key=int))

        # next search will begin from the previous oldest id
        params = {**params, "max_id": max_id}


async def delete_message(
        session: requests.Session,
        pbar: tqdm,
        message: dict,
):
    channel_id = message["channel"]
    message_id = message["id"]
    chat_kind = message["kind"]

    log_str = f"| message {message_id} | {chat_kind[:-1]} {channel_id} |"

    while True:
        path = f"channels/{channel_id}/messages/{message_id}"
        resp = session.delete(f"{BASE_URL}/{path}")

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
            logger.info(f"rate limit {retry_after}ms {log_str}")
            await asyncio.sleep(retry_after / 1000)
            continue

        if resp.status_code == 404:
            return

        logger.info(f"deleted {log_str}")
        pbar.update(1)
        return


async def delete_message_consumer(
        session: requests.Session,
        pbar: tqdm,
        queue: asyncio.Queue,
):
    while True:
        message = await queue.get()
        await delete_message(session, pbar, message)
        queue.task_done()


async def main():
    token = ""
    if args.token:
        token = args.token
    else:
        token = os.environ.get("DISCORD_TOKEN", None)

    if not token:
        raise Exception("no token has been provided")

    session = requests.Session()
    session.headers.update({"AUTHORIZATION": token})

    resp = session.get(ME_USERS_URL)
    if resp.status_code == 403 or resp.status_code == 401:
        raise Exception("user not authorized. invalid token.")

    user = resp.json()
    guilds = session.get(ME_USERS_GUILDS_URL).json()
    channels = session.get(ME_USERS_CHANNELS_URL).json()

    channels = [
        {**channel, "kind": ChatKind.Channel}
        for channel in channels
    ]

    guilds = [
        {**guild, "kind": ChatKind.Guild}
        for guild in guilds
    ]

    chats = channels + guilds

    print("searching your messages... this could take a few moments...")
    total_messages = 0
    with tqdm(total=len(chats)) as pbar:
        for chat in chats:
            result = await search_messages(session, chat, user)
            total_messages += result["total_results"]
            pbar.update(1)

    if total_messages == 0:
        print("you don't have any messages left")
        return

    print("starting to delete messages... this could take a LOT of time!")

    tasks = []
    queue = asyncio.Queue()
    pbar = tqdm(total=total_messages)

    # FIXME this task is taking too long to start deleting messages
    tasks.append(asyncio.create_task(
        delete_message_consumer(session, pbar, queue)))

    tasks.extend([
        asyncio.create_task(
            search_messages_worker(session, chat, user, queue))
        for chat in chats
    ])

    await asyncio.gather(*tasks)
    await queue.join()
    pbar.close()
    print(f"Deleted {total_messages} messages!")

asyncio.run(main())
