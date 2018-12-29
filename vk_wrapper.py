import random
import sys
import vk_api

def twoFactorHandler():
    code = input('Code: ')
    return code

def auth_user(login, password):
    vk_session = vk_api.VkApi(login, password, auth_handler=twoFactorHandler)
    try:
        vk_session.auth()
    except vk_api.AuthError as error_msg:
        print(error_msg)
        return None
    return vk_session

def auth_group(token):
    return vk_api.VkApi(token=token)


def getId(event):
    if event.from_user:
        return event.user_id
    elif event.from_chat:
        return event.chat_id


def sendMessage(vk, id, msg, keyboard=None, attachment=None):
    try:
        vk.messages.send(
            user_id=id,
            random_id=random.randint(0, sys.maxsize),
            message=msg,
            keyboard=keyboard,
            attachment=attachment
        )
    except Exception as e:
        print("Error, while sending message to user " + str(id) + ": " + str(e))


class MyEvent(object):
    def __init__(self, text, user_id):
        self.text = text
        self.user_id = user_id
