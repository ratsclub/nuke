import os
import asyncio
from itertools import chain
from tqdm import tqdm
import requests

BASE_URL = "https://discord.com/api"
ME_USERS_URL = f"{BASE_URL}/users/@me"
ME_USERS_GUILDS_URL = f"{BASE_URL}/users/@me/guilds"


async def search_messages(
        session: requests.Session,
        guild: dict,
        user: dict,
        params: dict = {},
):
    guild_id = guild["id"]
    user_id = user["id"]

    params = {**params, "author_id": user_id}
    path = f"guilds/{guild_id}/messages/search"

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
        guild: dict,
        user: dict,
        queue: asyncio.Queue,
        params: dict = {},
):

    params = {}
    while True:
        result = await search_messages(session, guild, user, params)
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

    resp = session.get(f"{BASE_URL}/users/@me")
    if resp.status_code == 403 or resp.status_code == 401:
        raise Exception("user not authorized. invalid token.")

    user = resp.json()
    guilds = session.get(f"{BASE_URL}/users/@me/guilds").json()

    messages_queue = asyncio.Queue()

    # get the sum of all messages
    total_messages = 0
    for guild in guilds:
        result = await search_messages(session, guild, user)
        total_messages += result["total_results"]

    if total_messages == 0:
        print("you don't have any messages left")
        return

    pbar = tqdm(total=total_messages)
    messages_queue = asyncio.Queue()

    producers = [
        asyncio.create_task(
            search_messages_worker(session, guild, user, messages_queue))
        for guild in guilds
    ]

    consumer = asyncio.create_task(
        delete_message_consumer(session, pbar, messages_queue))

    await asyncio.gather(*producers)
    await messages_queue.join()
    consumer.cancel()
    pbar.close()


asyncio.run(main())
