import os
import asyncio
from enum import Enum
from itertools import chain
import requests
from tqdm import tqdm

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

    params = {**params, "author_id": user_id}
    path = f"{chat_kind}/{chat_id}/messages/search"

    while True:
        resp = session.get(
            f"{BASE_URL}/{path}",
            params=params
        )

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
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
    while True:
        result = await search_messages(session, chat, user, params)
        total = result["total_results"]
        messages = result["messages"]

        # no messages left
        if total == 0:
            return

        # getting my own messages
        messages = [msg for msg in chain(*messages) if "hit" in msg]

        # sending messages to queue
        for msg in messages:
            await queue.put({
                "id": msg["id"],
                "channel": msg["channel_id"]
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

    while True:
        path = f"channels/{channel_id}/messages/{message_id}"
        resp = session.delete(f"{BASE_URL}/{path}")

        if resp.status_code == 429:
            retry_after = resp.json()["retry_after"]
            await asyncio.sleep(retry_after / 1000)
            continue

        if resp.status_code == 404:
            return

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
    for chat in chats:
        if chat["kind"] == ChatKind.Channel:
            recipients = [
                recipient["username"]
                for recipient in chat["recipients"]
                if recipient["id"] != user["id"]
            ]
            print("{}: {}".format(
                chat["id"],
                ", ".join(recipients)))
        else:
            print("{}: {}".format(chat["id"], chat["name"]))

        result = await search_messages(session, chat, user)
        total_messages += result["total_results"]

    if total_messages == 0:
        print("you don't have any messages left")
        return

    pbar = tqdm(total=total_messages)
    messages_queue = asyncio.Queue()

    print("starting to delete messages... this could take a LOT of time!")
    producers = [
        asyncio.create_task(
            search_messages_worker(session, chat, user, messages_queue))
        for chat in chats
    ]

    consumer = asyncio.create_task(
        delete_message_consumer(session, pbar, messages_queue))

    await asyncio.gather(*producers)
    await messages_queue.join()
    consumer.cancel()
    pbar.close()


asyncio.run(main())
