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
    "level=%(levelname)s msg=$(message)s at=%(funcName)s when=%(asctime)s",
    "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

URL_BASE = "https://discord.com/api"
URL_USERS_ME = "{}/users/@me".format(URL_BASE)
URL_USERS_GUILDS = "{}/users/@me/guilds".format(URL_BASE)
URL_USERS_CHANNELS = "{}/users/@me/channels".format(URL_BASE)
URL_SEARCH_PAGE = "{}/{}/{}/messages/search"
URL_DELETE_MESSAGE = "{}/channels/{}/messages/{}"

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
    chat_kind = chat["kind"]
    user_id = user["id"]

    params = {**params, "author_id": user_id, "include_nsfw": True}

    while True:
        resp = session.get(
            URL_SEARCH_PAGE
            .format(URL_BASE, chat_kind, chat_id),
            params=params
        )

        logger.info(
            "search on {} {} params={}"
            .format(chat_kind, chat_id, params)
        )

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
            logger.warning(
                "rate limited on {} {} - {}ms"
                .format(chat_kind[:-1], chat_id, retry_after)
            )
            await asyncio.sleep(retry_after / 1000)
            continue

        result = resp.json()
        if "total_results" not in result:
            return search_messages(session, chat, user, params)

        total_results = result["total_results"]

        if total_results == 0:
            return result

        # a message must be of type 0 to be deletable
        messages = result["messages"]
        messages = [
            msg for msg in chain(*messages)
            if "hit" in msg and msg["type"] == 0
        ]

        # it means that the page does not have deletable messages
        if len(messages) == 0:
            result["total_results"] = 0

        result["messages"] = messages

        return result


async def search_messages_worker(
        session: requests.Session,
        chat: dict,
        user: dict,
        pbar: tqdm,
        params: dict = {},
):
    params = {}
    chat_kind = chat["kind"]
    chat_id = chat["id"]

    while True:
        result = await search_messages(session, chat, user, params)
        total_results = result["total_results"]
        messages = result["messages"]

        # no messages left
        if total_results == 0:
            logger.info(
                "done on {} {}"
                .format(chat_kind, chat_id)
            )
            return

        # getting the oldest id
        ids = [msg["id"] for msg in messages]
        max_id = min(sorted(ids, key=int))

        # next search will begin from the previous oldest id
        params = {**params, "max_id": max_id}

        messages_tasks = [
            asyncio.create_task(delete_message(session, pbar, {
                "id": msg["id"],
                "channel": msg["channel_id"],
                "kind": chat_kind
            }))
            for msg in messages
        ]

        await asyncio.gather(*messages_tasks)


async def delete_message(
        session: requests.Session,
        pbar: tqdm,
        message: dict,
):
    channel_id = message["channel"]
    message_id = message["id"]

    while True:
        resp = session.delete(
            URL_DELETE_MESSAGE
            .format(URL_BASE, channel_id, message_id)
        )

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
            logger.warning(
                "rate limited on channel {} message {} - {}ms"
                .format(channel_id, message_id, retry_after)
            )
            await asyncio.sleep(retry_after / 1000)
            continue

        if resp.status_code == 404:
            return

        if resp.status_code == 204:
            pbar.update(1)
            return


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

    resp = session.get(URL_USERS_ME)
    if resp.status_code == 403 or resp.status_code == 401:
        raise Exception("user not authorized. invalid token.")

    user = resp.json()
    guilds = session.get(URL_USERS_GUILDS).json()
    channels = session.get(URL_USERS_CHANNELS).json()

    channels = [
        {**channel, "kind": ChatKind.Channel.value}
        for channel in channels
    ]

    guilds = [
        {**guild, "kind": ChatKind.Guild.value}
        for guild in guilds
    ]

    tmp_chats = channels + guilds
    pbar = tqdm(total=len(tmp_chats))
    pbar.set_description("counting messages")

    # FIXME use only one list, don't make another copy
    chats = []
    total_messages = 0
    for chat in tmp_chats:
        result = await search_messages(session, chat, user)

        total_results = result["total_results"]

        # don't search again if it has no messages
        if total_results == 0:
            pbar.update(1)
            continue

        total_messages += total_results
        chats.append(chat)

        pbar.update(1)

    if total_messages == 0:
        pbar.close()
        print("you don't have any messages left")
        return

    tasks = []
    pbar.reset()
    pbar.total = total_messages
    pbar.set_description("deleting")
    tasks = [
        asyncio.create_task(
            search_messages_worker(session, chat, user, pbar))
        for chat in chats
    ]

    await asyncio.gather(*tasks)
    pbar.set_description("Done!")
    pbar.close()

asyncio.run(main())
