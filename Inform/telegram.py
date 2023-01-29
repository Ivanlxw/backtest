import requests


def telegram_bot_sendtext(bot_message, bot_apikey, bot_chatid):
    send_text = 'https://api.telegram.org/bot' + bot_apikey + \
        '/sendMessage?chat_id=' + bot_chatid + \
        '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    return response.json()


def telegram_bot_getchat_id(bot_apiKey):
    url = f"https://api.telegram.org/bot{bot_apiKey}/getUpdates"
    response = requests.get(url)
    if response.ok:
        print(response.json())
        return response.json()["chat"]["id"]
