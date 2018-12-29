import schedule
import datetime
import threading
import time
import json

import pymysql

def connectDB(host, user, password, db, charset="utf8"):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 passwd=password,
                                 db=db,
                                 charset=charset,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection, connection.cursor()

INSERT_POST_QUERY = "INSERT INTO `posts` (senderUserID, user_id, post_id, comments) VALUES(%s,%s,%s, '')"
FIND_POST_QUERY = "SELECT * FROM `posts`"

usersPerGroup = 15
judgesPerGroup = 3

groupId = None
groupLink = None

db_host = "localhost"
db_user = "user"
db_password = "password"
db_dbName = "vkBot"

from vk_wrapper import *
from vk_api.longpoll import VkLongPoll, VkEventType

token = ""
login = ""
password = ""

keyboards = {
    "menu":  "./keyboard_menu.json",
    "nextOrCancel": "./keyboard_nextOrCancel.json",
    "start": "./keyboard_start.json",
    "cancel": "./keyboard_cancel.json",
    "next": "./keyboard_next.json",
    "admin": "./keyboard_admin.json",
    "empty": "./keyboard_empty.json"
}

postTimeMin = datetime.time(0, 0, 0)
postTimeMax = datetime.time(23, 59, 59)

perfTskTimeMin = datetime.time(0, 22, 0)
perfTskTimeMax = datetime.time(23, 59, 59)

fine_notPosted = 3
fine_notLiked = 5
fine_notCommented = 4
fine_notAppreciate = 5
fine_notSent = 3
notLikedPercent = 60

lazy_fine = 10

admins = []

with open('config.json') as file:
    cfg = json.load(file)

usersPerGroup = cfg["groups"]["usersPerGroup"]
judgesPerGroup = cfg["groups"]["judgesPerGroup"]
groupId = cfg["vkGroup"]["groupId"]
groupLink = cfg["vkGroup"]["groupLink"]
db_host = cfg["db"]["host"]
db_user = cfg["db"]["user"]
db_password = cfg["db"]["password"]
db_dbName = cfg["db"]["dbName"]
token = cfg["vk"]["token"]
login = cfg["vk"]["login"]
password = cfg["vk"]["password"]
fine_notPosted = cfg["fines"]["notPosted"]
fine_notLiked = cfg["fines"]["notLiked"]
fine_notAppreciate = cfg["fines"]["notAppreciate"]
fine_notCommented = cfg["fines"]["notCommented"]
fine_notSent = cfg["fines"]["notSent"]
notLikedPercent = cfg["fines"]["notLikedPercent"]
lazy_fine = cfg["fines"]["4lazy"]
admins = cfg["admins"]

def loadKeyboards():
    for key in keyboards:
        keyboards[key] = open(keyboards[key], "r", encoding="UTF-8").read()


def getKeyboard4user(id):
    if id in admins:
        return keyboards["admin"]
    else:
        return keyboards["menu"]


def addPost(id, userId, postId):
    global dbCursor, dbConnection

    dbCursor.execute("INSERT INTO `posts` (senderUserID, user_id, post_id, comments) VALUES(%s,%s,%s, '')", (id, userId, postId))
    dbConnection.commit()


def findPost(id=None, userId=None, postId=None, showed=None, date=None, custom=None):
    global dbCursor
    query = FIND_POST_QUERY
    query += " WHERE "
    blankQ = query
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = -1")
    banned = dbCursor.fetchall()
    if len(banned) > 0:
        query += "("
    for u in banned:
        query += "senderUserID != " + str(u["vk_id"]) + " AND "
    if len(banned) > 0:
        query = query[:-4] + ")"
    if id is not None:
        if query != blankQ:
            query += " AND "
        query += " id = " + id
    if userId is not None:
        if query != blankQ:
            query += " AND "
        query += " user_id = " + userId
    if postId is not None:
        if query != blankQ:
            query += " AND "
        query += " post_id = " + postId
    if showed is not None:
        if query != blankQ:
            query += " AND "
        query += " showed = " + showed
    if date is not None:
        if query != blankQ:
            query += " AND "
        query += " date = " + date
    if custom is not None:
        if query != blankQ:
            query += " AND "
        query += custom
    dbCursor.execute(query)
    return dbCursor.fetchall()


def addUserStats(vkId, liked=0, commented=0, fine=0):
    if liked == 0 and commented == 0 and fine == 0:
        return None
    query = "UPDATE `users` SET postsLiked = postsLiked + %s" \
            + ", postsCommented = postsCommented + %s" \
            + ", fine = fine + %s WHERE vk_id = %s"
    global dbCursor, dbConnection
    dbCursor.execute(query, (liked, commented, fine, vkId))
    dbConnection.commit()
    return dbCursor.fetchall()


def getTimeDiff(a, b):
    diff = b - a
    return divmod(diff.days * 86400 + diff.seconds, 60)


def deferCall(fn, delay,  *args):
    time.sleep(delay)
    fn(args)


def distributeUsers():
    global dbCursor, dbConnection
    dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
    maxGroupNo = dbCursor.fetchall()[0]["groupNo"]
    dbCursor.execute("SELECT MIN(groupNo) AS groupNo FROM `users`")
    minGroupNo = dbCursor.fetchall()[0]["groupNo"]
    for i in range(minGroupNo, maxGroupNo+1):
        query = "SELECT * FROM `users` WHERE groupNo = %s AND groupNo != -1"
        dbCursor.execute(query, (i))
        members = dbCursor.fetchall()
        ids = []
        for m in members:
            ids.append(m["ID"])
        judges = random.sample(ids, judgesPerGroup)
        query = "UPDATE `users` SET groupRole = %s WHERE ID=%s"
        for member in members:
            role = 0
            if member["ID"] in judges:
                role = 1
            dbCursor.execute(query, (role, member["ID"]))
    dbConnection.commit()


def groupUsers():
    global dbCursor, dbConnection
    dbCursor.execute("SELECT * FROM `users` WHERE groupNo != -1 AND groupNo != 0")
    users = dbCursor.fetchall()
    ids = []
    for u in users:
        ids.append(u["ID"])
    random.shuffle(ids)
    i, g = 0, 1
    query = "UPDATE `users` SET groupNo = %s WHERE ID = %s"
    for id in ids:
        if i % usersPerGroup == 0 and i != 0:
            g += 1
        dbCursor.execute(query, (g, id))
        i += 1
    dbConnection.commit()


def groupAndDistribute():
    groupUsers()
    distributeUsers()
    groupEvenly()


def addAssessment(post_id, assessment):
    global dbCursor, dbConnection
    post = findPost(postId=post_id)[0]
    newAss = (post["assessment"] + assessment) / (post["assessmentsCount"] + 1)
    query = "UPDATE `posts` SET assessment = %s, assessmentsCount = assessmentsCount + 1 WHERE post_id = %s"
    dbCursor.execute(query, (newAss, post_id))
    dbConnection.commit()


def addComment(post_id, comment):
    global dbCursor, dbConnection
    post = findPost(postId=post_id)[0]
    query = "UPDATE `posts` SET comments = %s WHERE post_id = %s"
    dbCursor.execute(query, (post["comments"] + "\n" + comment, post_id))
    dbConnection.commit()


def findUser(user_id):
    global dbCursor
    query = "SELECT * FROM `users` WHERE vk_id = %s"
    dbCursor.execute(query, (user_id))
    return dbCursor.fetchall()


def getRole(user_id):
    query = "SELECT groupNo FROM `users` WHERE vk_id = %s"
    dbCursor.execute(query, (user_id))
    groupNo = dbCursor.fetchall()[0]["groupNo"]
    return groupNo


def start(vk, event):
    global dbCursor, dbConnection
    id = event.user_id
    isMember = vk2.groups.isMember(user_id=id, group_id=groupId)
    if not isMember:
        sendMessage(vk, id, "Вы должны встпутить в группу " + groupLink, keyboard=keyboards["start"])
        return
    query = "SELECT MAX(groupNo) AS groupNo FROM `users`"
    dbCursor.execute(query)
    groupNo = dbCursor.fetchall()[0]["groupNo"]
    if groupNo is None:
        groupNo = 0
    query = "SELECT ID FROM `users` WHERE groupNo = %s"
    dbCursor.execute(query, (groupNo))
    members = dbCursor.fetchall()
    if len(members) == usersPerGroup:
        groupNo += 1
    query = "INSERT IGNORE INTO `users` (vk_id, groupNo) VALUES (%s, %s)"
    dbCursor.execute(query, (str(id), groupNo))
    dbConnection.commit()
    sendMessage(vk, id, "Здравствуйте!", keyboard=keyboards["menu"])


postSent = []
def sendPost(vk, event):
    global cmdNow, postSent, vk2
    id = getId(event)
    if event.text == "Отмена":
        sendMessage(vk, id, "Выберите действие", keyboard=getKeyboard4user(id))
        if id in cmdNow: del cmdNow[id]
        if id in postSent:
            postSent.remove(id)
            return
    if not id in admins:
        now = datetime.datetime.now()
        if now < now.replace(hour=postTimeMin.hour, minute=postTimeMin.minute, second=postTimeMin.second) or \
                now > now.replace(hour=postTimeMax.hour, minute=postTimeMax.minute, second=postTimeMax.second):
            sendMessage(vk, id, "Вы можете отправлять пост с " + str(postTimeMin.hour) + ":" + str(postTimeMin.minute)
                        + " до " + str(postTimeMax.hour) + ":" + str(postTimeMax.minute))
            return
        for post in findPost(userId=str(event.user_id)):
            date = post["date"]
            if date.day == datetime.datetime.now().day:
                sendMessage(vk, id, "Вы сегодня уже отправляли пост")
                return

    cmdNow[id] = sendPost
    if not id in postSent:
        sendMessage(vk, id, "Отправьте ссылку на ваш пост", keyboard=keyboards["cancel"])
        postSent.append(id)
    else:
        if len(event.attachments) > 0:
            postInfoStr = event.attachments["attach1"]
        elif event.text and "vk.com/wall" in event.text:
            postInfoStr = event.text
            sub = ""
            if "https://" in event.text:
                sub = "https://"
            sub += "vk.com/wall"
            postInfoStr = postInfoStr.replace(sub, "")
        else:
            sendMessage(vk, id, "Отправте ссылку на пост или сам пост", keyboard=keyboards["cancel"])
            return
        postInfo = postInfoStr.split("_")
        userId = postInfo[0]
        postId = postInfo[1]
        if userId == str(event.user_id) or id in admins:
            if not id in admins:
                posts = vk2.wall.getById(posts=userId + "_" + postId)
                ts = int(posts[0]["date"])
                postDate = datetime.datetime.utcfromtimestamp(ts)
                postDate += datetime.timedelta(hours=5)
                if getTimeDiff(postDate, datetime.datetime.now())[0] > 1440:
                    sendMessage(vk, id, "Вы не можете отправлять посты старше 24 часов", keyboard=getKeyboard4user(id))
                    return
            addPost(id, userId, postId)
            del cmdNow[id]
            postSent.remove(id)
            sendMessage(vk, id, "Ваш пост принят!", keyboard=getKeyboard4user(id))
            return
        else:
            sendMessage(vk, id, "Отправьте пост со своей страницы")


taskSent = {}
waitingSend = []
def performTask(vk, event, shouldCheck = True):
    global cmdNow, taskSent, dbCursor, dbConnection
    id = event.user_id
    if id in waitingSend:
        sendMessage(vk, id, "Следующий пост будет отправлен через 15 секунд...")
        return
    if shouldCheck:
        now = datetime.datetime.now()
        if now < now.replace(hour=perfTskTimeMin.hour, minute=perfTskTimeMin.minute, second=perfTskTimeMin.second) or \
                now > now.replace(hour=perfTskTimeMax.hour, minute=perfTskTimeMax.minute, second=perfTskTimeMax.second):
            sendMessage(vk, id, "Вы можете выполнять задания с " + str(perfTskTimeMin.hour) + ":" + str(perfTskTimeMin.minute)
                        + " до " + str(perfTskTimeMax.hour) + ":" + str(perfTskTimeMax.minute), keyboard=keyboards["menu"])
            if id in cmdNow: del cmdNow[id]
            return
        if event.text == "Отмена":
            sendMessage(vk, id, "Выберите действие", keyboard=getKeyboard4user(id))
            if id in cmdNow: del cmdNow[id]
            if id in waitingSend:
                waitingSend.remove(id)
                return
    if not id in taskSent:
        posts = []
        if shouldCheck:
            if datetime.datetime.today().weekday() == 6:
                if len(admins) != 0:
                    query = "SELECT * FROM `posts` WHERE TO_DAYS(NOW()) - TO_DAYS(DATE(date)) = 0 AND ("
                    for a in admins:
                        query += " senderUserID = " + str(a) + " or "
                    query = query[:-3] if len(admins) != 0 else query
                    query += ")"
                    if dbCursor.execute(query) == 0:
                        sendMessage(vk, id, "Вы не можете выполнять задания в воскресенье", keyboard=keyboards["menu"])
                        if id in cmdNow: del cmdNow[id]
                        return
                    else:
                        posts = dbCursor.fetchall()
                else:
                    sendMessage(vk, id, "Вы не можете выполнять задания в воскресенье", keyboard=keyboards["menu"])
                    if id in cmdNow: del cmdNow[id]
                    return
        cmdNow[id] = performTask
        adminsPosts = []
        if len(admins) != 0:
            query = "SELECT * FROM `posts` WHERE TO_DAYS(NOW()) - TO_DAYS(DATE(date)) = 0 AND ("
            for a in admins:
                query += " senderUserID = " + str(a) + " or "
            query = query[:-3] + ")"
            dbCursor.execute(query)
            adminsPosts = dbCursor.fetchall()
        if len(adminsPosts) == 0:
            dbCursor.execute("SELECT groupNo FROM `users` WHERE vk_id = %s", (event.user_id))
            dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = %s", dbCursor.fetchall()[0]["groupNo"])
            query = "TO_DAYS(NOW()) - TO_DAYS(DATE(date)) = 0 AND ("
            for u in dbCursor.fetchall():
                query += "senderUserID = " + str(u["vk_id"]) + " or "
            query = query[:-3] + ") AND senderUserID != " + str(id)
            if len(posts) == 0:
                posts = findPost(custom=query)
            if len(posts) == 0:
                sendMessage(vk, id, "Не нашлось ни одного поста.\n"
                                    "Пожалуйста, подождите, пока пользователи отправят новые посты.",
                            keyboard=keyboards["menu"])
                if id in cmdNow: del cmdNow[id]
                return
        else:
            posts = adminsPosts
        rndPost = random.choice(posts)
        ok = False
        i = 0
        while not ok and i < len(posts):
            isliked = vk2.likes.getList(type="post", owner_id=rndPost["user_id"], item_id=rndPost["post_id"])
            if id in isliked["items"]:
                rndPost = random.choice(posts)
                i += 1
            else:
                ok = True
        if not ok:
            sendMessage(vk, id, "Не нашлось ни одного поста.\n"
                                "Пожалуйста, подождите, пока пользователи отправят новые посты.",
                        keyboard=keyboards["menu"])
            if id in cmdNow: del cmdNow[id]
            return
        postId = "wall"+rndPost["user_id"]+"_"+rndPost["post_id"]
        shouldComment = random.choices([True, False], weights=[10, 90])[0]
        text = "Вот пост. Поставте на него like"
        if shouldComment:
            text += " и оставте комментарий"
        sendMessage(vk, id, text + ".\n"
                                   "Ссылка: " + "https://vk.com/"+postId,
                    attachment=postId, keyboard=keyboards["next"])
        taskSent[id] = (rndPost["user_id"]+"_"+rndPost["post_id"], shouldComment)
        dbCursor.execute("UPDATE `posts` SET showed = showed + 1 WHERE ID = %s", (rndPost["ID"]))
        dbConnection.commit()
    else:
        ownerid, postid = taskSent[id][0].split("_")
        commented = False
        if taskSent[id][1]:
            comments = vk2.wall.getComments(owner_id=ownerid, post_id=postid, preview_length=1, sort="desc")["items"]
            for c in comments:
                if c["from_id"] == event.user_id:
                    commented = True
                    break
        isliked = id in vk2.likes.getList(type="post", owner_id=ownerid, item_id=postid)["items"]
        addUserStats(vkId=event.user_id,
                     liked=1 if isliked else 0,
                     commented=1 if taskSent[id][1] and commented else 0,
                     )
        text = "Вы не выполнили предыдущее задание:\n"
        if not isliked:
            text += "1) Поставьте лайк"
        if not commented and taskSent[id][1]:
            text += "\n2) Оставьте камментарий!"
        if not isliked or (not commented and taskSent[id][1]):
            sendMessage(vk, id, text + "\nhttps://vk.com/wall"+ownerid + "_" + postid)
            return

        dbCursor.execute("SELECT groupRole FROM `users` WHERE vk_id = %s", (event.user_id))
        isJudge = dbCursor.fetchall()[0]["groupRole"] == 1
        if isJudge:
            judge(vk, event)
        else:
            sendMessage(vk, id, "Сейчас отправлю следующий пост...", keyboard=keyboards["cancel"])
            del taskSent[id]
            waitingSend.append(id)
            threading.Thread(target=deferCall, args=(performTask2, 15, vk, event,)).start()
def performTask2(args):
    if args[1].user_id in waitingSend:
        waitingSend.remove(args[1].user_id)
        performTask(args[0], args[1])


def safeS2i(str):
    try:
        return int(str)
    except:
        return None

assessmentSent = {}
def judge(vk, event):
    global cmdNow
    id = getId(event)
    cmdNow[id] = judge
    if id not in assessmentSent:
        sendMessage(vk, id, "Поставьте оценку посту (от 1 до 10)", keyboard=keyboards["empty"])
        assessmentSent[id] = None
    else:
        ownerid, postid = taskSent[id][0].split("_")
        if assessmentSent[id]:
            if event.text and event.text != "Далее":
                addComment(postid, event.text)
                sendMessage(vk, id, "Ваш комментарий принят!", keyboard=getKeyboard4user(id))
            else:
                sendMessage(vk, id, "Вы получили штрафные очки: " + str(fine_notCommented))
                addUserStats(id, fine=fine_notCommented)
            del assessmentSent[id]
            sendMessage(vk, id, "Сейчас отправлю следующий пост...", keyboard=keyboards["cancel"])
            del taskSent[id]
            waitingSend.append(id)
            threading.Thread(target=deferCall, args=(performTask2, 15, vk, event,)).start()
            cmdNow[id] = performTask
            return
        assessment = safeS2i(event.text)
        if assessment is None or assessment > 10 or assessment < 1:
            sendMessage(vk, id, "Поставьте оценку посту (от 1 до 10)!")
            return
        addAssessment(postid, assessment)
        sendMessage(vk, id, "Ваша оценка принята!", keyboard=getKeyboard4user(id))
        if assessmentSent[id] is None:
            shouldComment = random.choices([True, False], weights=[100, 100])[0]
            if shouldComment:
                sendMessage(vk, id, "Что вам понравилось и не понравилось в посте?"
                                    " (сообщение будет анонимно передано автору поста)", keyboard=keyboards["empty"])
                assessmentSent[id] = True
                return
        del assessmentSent[id]
        sendMessage(vk, id, "Сейчас отправлю следующий пост...", keyboard=keyboards["cancel"])
        del taskSent[id]
        waitingSend.append(id)
        threading.Thread(target=deferCall, args=(performTask2, 15, vk, event,)).start()
        cmdNow[id] = performTask

def getBestUsersInGroups2():
    global dbCursor, dbConnection, vk
    dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
    maxGroupNo = dbCursor.fetchall()[0]["groupNo"]
    dbCursor.execute("SELECT MIN(groupNo) AS groupNo FROM `users`")
    minGroupNo = dbCursor.fetchall()[0]["groupNo"]
    query = "SELECT * FROM `users` WHERE" \
            "(`postsLiked` = (SELECT MAX(`postsLiked`) FROM users) " \
            "or `postsCommented` = ( SELECT MAX(`postsCommented`) FROM users)" \
            "or `fine` = (SELECT MIN(`fine`) FROM users)) AND `groupNo` = %s AND groupNo != -1"
    res = []
    for i in range(minGroupNo, maxGroupNo + 1):
        dbCursor.execute(query, (i))
        users = dbCursor.fetchall()
        maxSum = 0
        maxUsers = []
        for u in users:
            sum = u["postsLiked"] + u["postsCommented"]
            if sum > maxSum:
                maxSum = sum
                maxUsers.clear()
                maxUsers.append(u)
            elif sum == maxSum:
                maxUsers.append(u)
        if len(maxUsers) == 1:
            res.append(maxUsers)
            continue
        minSum = None
        minUsers = []
        for u in minUsers:
            if minSum is None or u["fine"] < minSum:
                minSum = u["fine"]
                minUsers.clear()
                minUsers.append(u)
            elif u["fine"] == minSum:
                minUsers.append(u)
        res.append(minUsers)
    for gU in res:
        for u in gU:
            dbCursor.execute("UPDATE users SET isBestInGroup = 1 WHERE ID = %s", (u["ID"]))
            try:
                sendMessage(vk, u["vk_id"], "Поздравляем, Вы лучший в группе!")
            except:
                pass
    dbConnection.commit()
    return res


def list_duplicates_of(seq,item):
    start_at = -1
    locs = []
    while True:
        try:
            loc = seq.index(item,start_at+1)
        except ValueError:
            break
        else:
            locs.append(loc)
            start_at = loc
    return locs


def getBestUsersInGroups():
    global dbCursor, dbConnection, vk
    dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
    maxGroupNo = dbCursor.fetchall()[0]["groupNo"]
    dbCursor.execute("SELECT MIN(groupNo) AS groupNo FROM `users`")
    minGroupNo = dbCursor.fetchall()[0]["groupNo"]
    for i in range(minGroupNo, maxGroupNo + 1):
        dbCursor.execute("SELECT * FROM `users` WHERE groupNo = %s", (i))
        sums = []
        users = dbCursor.fetchall()
        for u in users:
            sums.append(getLikedPercent(u["vk_id"]) + u["postsCommented"])
        bests = []
        print(i, sums)
        if sums.count(max(sums)) > 1:
            indxs = list_duplicates_of(sums, max(sums))
            fines = []
            for i in indxs:
                fines.append(users[i]["fine"])
            if fines.count(min(fines)) == 1:
                bests.append(users[indxs[fines.index(min(fines))]])
            else:
                ldf = list_duplicates_of(fines, min(fines))
                for i in ldf:
                    bests.append(users[indxs[i]])
        else:
            bests.append(users[sums.index(max(sums))])
        for b in bests:
            dbCursor.execute("UPDATE `users` SET isBestInGroup = 1 WHERE ID = %s", (b["ID"]))
            sendMessage(vk, b["vk_id"], "Поздравляем, Вы лучший в группе!")
    dbConnection.commit()


def getUserStatsStr(id):
    global dbCursor
    res = ""
    dbCursor.execute("SELECT postsLiked, postsCommented, fine FROM users"
                     " WHERE vk_id = %s", (id))
    stats = dbCursor.fetchall()[0]
    poststNum = len(findPost(custom="senderUserID != "+str(id)+" AND TO_DAYS(NOW()) - TO_DAYS(DATE(date)) <= 7"))
    res += "Вы лайкнули " + str(stats["postsLiked"]) + " из " + str(poststNum)
    res += " - " + str(int(getLikedPercent(id))) + "% (минимум - " + str(notLikedPercent) + "%)\n"
    res += "Вы прокомментировали " + str(stats["postsCommented"]) + "\n"
    res += "Количество штрафных очков: " + str(stats["fine"])
    return res


def getUserPostsStatsStr(id):
    global dbCursor
    res = ""
    dbCursor.execute("SELECT * FROM posts WHERE senderUserID = %s", (id))
    for p in dbCursor.fetchall():
        res += str(p["date"]) + ": https://vk.com/wall" + str(p["user_id"]) + "_" + str(p["post_id"]) + "\n"
        res += "Показан раз: " + str(p["showed"]) + "\n"
        res += "Средняя оценка: " + str(round(p["assessment"], 1)) + "\n"
        res += "Комментарии:\n" + p["comments"] + "\n\n"
    return res if res != "" else "У Вас нет ни одного поста!"


def clearStats():
    global dbCursor, dbConnection, vk
    dbCursor.execute("SELECT ID, vk_id FROM users")
    query = "UPDATE `users` SET postsLiked = 0, postsCommented = 0, " \
            "fine = 0, isBestInGroup = 0 WHERE ID = %s"
    for u in dbCursor.fetchall():
        try:
            sendMessage(vk, u["vk_id"], getUserStatsStr(u["vk_id"]))
            sendMessage(vk, u["vk_id"], getUserPostsStatsStr(u["vk_id"]))
        except:
            pass
        dbCursor.execute(query, (u["ID"]))
    dbConnection.commit()


def giveFines():
    global dbCursor, vk2
    dbCursor.execute("SELECT * FROM `users` WHERE groupNo != -1")
    users = dbCursor.fetchall()
    for u in users:
        try:
            post = vk2.wall.get(owner_id=u["vk_id"], count=1, filter="owner")["items"]
        except:
            continue
        if len(post) != 0:
            postDate = datetime.datetime.utcfromtimestamp(int(post[0]["date"]))
            postDate += datetime.timedelta(hours=5)
            if getTimeDiff(postDate, datetime.datetime.now())[0] > 2880:
                addUserStats(u["vk_id"], fine=fine_notPosted)
        else:
            addUserStats(u["vk_id"], fine=fine_notPosted)

        dbCursor.execute("SELECT date FROM `posts` WHERE user_id = %s LIMIT 1", (u["vk_id"]))
        sentPosts = dbCursor.fetchall()
        if len(sentPosts) != 0:
            if sentPosts[0]["date"].day != datetime.datetime.now().day:
                addUserStats(u["vk_id"], fine=fine_notSent)
        else:
            addUserStats(u["vk_id"], fine=fine_notSent)


def sendStats(vk, event):
    sendMessage(vk, event.user_id, getUserStatsStr(event.user_id))
    sendMessage(vk, event.user_id, getUserPostsStatsStr(event.user_id))


def getLikedPercent(id):
    global dbCursor
    dbCursor.execute("SELECT postsLiked, groupNo FROM `users` WHERE vk_id = %s", (id))
    user = dbCursor.fetchall()[0]
    query = "senderUserID != "+str(id)+" AND TO_DAYS(NOW()) - TO_DAYS(DATE(date)) <= 7"
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = %s", (user["groupNo"]))
    sameGroup = dbCursor.fetchall()
    if len(sameGroup) > 0:
        query += " AND ("
    for u in sameGroup:
        query += "senderUserID = " + str(u["vk_id"]) + " or "
    if len(sameGroup) > 0:
        query = query[:-3] + ")"
    poststNum = len(findPost(custom=query))
    liked = user["postsLiked"]
    if poststNum == 0:
        return 0.0
    return (liked / poststNum) * 100


def checkLikedPercent():
    global dbCursor
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo != -1")
    users = dbCursor.fetchall()
    for u in users:
        if getLikedPercent(u["vk_id"]) < notLikedPercent:
            addUserStats(u["vk_id"], fine=fine_notLiked)


def getLazy():
    global dbCursor, dbConnection, vk
    dbCursor.execute("SELECT * FROM `users`  WHERE groupNo != -1")
    for u in dbCursor.fetchall():
        if u["fine"] >= lazy_fine:
            dbCursor.execute("UPDATE `users` SET groupNo = 0 WHERE ID = %s", (u["ID"]))
            sendMessage(vk, u["vk_id"], "Вы попали в группу ленивцев!")
    dbConnection.commit()


def sendStatsAll():
    global dbCursor, vk
    dbCursor.execute("SELECT * FROM `users` WHERE groupNo != -1")
    for u in dbCursor.fetchall():
        try:
            sendMessage(vk, u["vk_id"], "Ваша статистика: ")
            sendMessage(vk, u["vk_id"], getUserStatsStr(u["vk_id"]))
            sendMessage(vk, u["vk_id"], getUserPostsStatsStr(u["vk_id"]))
        except:
            continue


def sendAdminStats(vk, event):
    global dbCursor, vk2
    id = event.user_id
    dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
    sendMessage(vk, id, "Всего групп: " + str(dbCursor.fetchall()[0]["groupNo"]))
    dbCursor.execute("SELECT ID FROM `users`")
    sendMessage(vk, id, "Всего пользователей: " + str(len(dbCursor.fetchall())))
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = -1")
    banned = dbCursor.fetchall()
    sendMessage(vk, id, "Пользователей заблокировано: " + str(len(banned)))
    res = ""
    for u in banned:
        user = vk2.users.get(user_ids=u["vk_id"])[0]
        res += user["first_name"] + " " + user["last_name"] + " (" + str(u["vk_id"]) + ")\n"
    if len(banned) != 0:
        sendMessage(vk, id, res)
    dbCursor.execute("SELECT ID FROM `posts` WHERE DATE(`date`) = CURDATE()")
    sendMessage(vk, id, "Постов за сегодня: " + str(len(dbCursor.fetchall())))
    dbCursor.execute("SELECT * FROM `users` WHERE groupNo = 0")
    res = ""
    lazy = dbCursor.fetchall()
    for u in lazy:
        user = vk2.users.get(user_ids=u["vk_id"])[0]
        res += user["first_name"] + " " + user["last_name"] + " (" + str(u["vk_id"]) + ")"
        res += ": " + str(u["fine"]) + "\n"
    sendMessage(vk, id, "Ленивцев: " + str(len(lazy)))
    if len(lazy) != 0:
        sendMessage(vk, id, res)


def sendAdminUserList(vk, event):
    global dbCursor
    dbCursor.execute("SELECT vk_id, groupNo FROM `users` WHERE groupNo != -1 ORDER BY `users`.`groupNo` ASC")
    res = ""
    id = event.user_id
    users = dbCursor.fetchall()
    sendMessage(vk, id, "Загружаю список пользователей....")
    lastGNo = -1
    for u in users:
        if lastGNo != u["groupNo"]:
            res += "Группа: " + (str(u["groupNo"]) if u["groupNo"] != 0 else "ленивцы") + ":\n"
            lastGNo = u["groupNo"]
        user = vk2.users.get(user_ids=u["vk_id"])
        if len(user) == 0:
            continue
        user = user[0]
        res += user["first_name"] + " " + user["last_name"] + ": " + str(u["vk_id"]) + "\n"
    if res != "":
        sendMessage(vk, id, res)
    else:
        sendMessage(vk, id, "Нет пользователей")
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = -1")
    res = ""
    for u in dbCursor.fetchall():
        user = vk2.users.get(user_ids=u["vk_id"])
        if len(user) == 0:
            continue
        user = user[0]
        res += user["first_name"] + " " + user["last_name"] + ": " + str(u["vk_id"]) + "\n"
    if res != "":
        sendMessage(vk, id, "Заблокировано:\n" + res)


idSent = []
def banUser(vk, event):
    global dbCursor, dbConnection
    id = event.user_id
    if event.text == "Отмена":
        if id in cmdNow: del cmdNow[id]
        if id in idSent: idSent.remove(id)
        sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])
        return
    cmdNow[id] = banUser
    if not id in idSent:
        sendMessage(vk, id, "Отправте ID пользователя, которого хотите заблокировать", keyboard=keyboards["cancel"])
        idSent.append(id)
    else:
        if event.text:
            if dbCursor.execute("UPDATE `users` SET groupNo = -1 WHERE vk_id = %s", (event.text)) == 0:
                sendMessage(vk, id, "Пользователь с ID = '" + event.text + "' не найден или уже заблокирован.",
                            keyboard=keyboards["admin"])
            else:
                sendMessage(vk, id, "Пользователь с ID = '" + event.text + "' заблокирован.",
                            keyboard=keyboards["admin"])
                sendMessage(vk, event.text, "Вы заблокированы администратором", keyboard=keyboards["empty"])
                print("Admin " + str(id) + " banned user " + event.text)
            del cmdNow[id]
            idSent.remove(id)
            dbConnection.commit()
        else:
            if id in cmdNow: del cmdNow[id]
            idSent.remove(id)
            sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])


def unblockUser(vk, event):
    global dbCursor, dbConnection
    id = event.user_id
    if event.text == "Отмена":
        if id in cmdNow: del cmdNow[id]
        if id in idSent: idSent.remove(id)
        sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])
        return
    cmdNow[id] = unblockUser
    if not id in idSent:
        sendMessage(vk, id, "Отправте ID пользователя, которого хотите раззаблокировать", keyboard=keyboards["cancel"])
        idSent.append(id)
    else:
        if event.text:
            if dbCursor.execute("UPDATE `users` SET groupNo = 1 WHERE vk_id = %s", (event.text)) == 0:
                sendMessage(vk, id, "Пользователь с ID = '" + event.text + "' не найден или не заблокирован.",
                            keyboard=keyboards["admin"])
            else:
                sendMessage(vk, id, "Пользователь с ID = '" + event.text + "' разблокирован.",
                            keyboard=keyboards["admin"])
                sendMessage(vk, event.text, "Вы разблокированы администратором", keyboard=keyboards["menu"])
                print("Admin " + str(id) + " unblocked user " + event.text)
                groupUsers()
            del cmdNow[id]
            idSent.remove(id)
            dbConnection.commit()
        else:
            if id in cmdNow: del cmdNow[id]
            idSent.remove(id)
            sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])


groupUsersSent = []
def manuallyCreateGroup(vk, event):
    global cmdNow, dbCursor, dbConnection
    id = event.user_id
    if event.text == "Отмена" or not event.text:
        if id in cmdNow: del cmdNow[id]
        if id in groupUsersSent: groupUsersSent.remove(id)
        sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])
        return
    cmdNow[id] = manuallyCreateGroup
    if id not in groupUsersSent:
        sendMessage(vk, id, "Напишите через запятую ID пользователей, которых хотите добавить в группу.", keyboard=keyboards["cancel"])
        groupUsersSent.append(id)
    else:
        users = event.text.split(",")
        dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
        groupNo = dbCursor.fetchall()[0]["groupNo"] + 1
        query = "UPDATE `users` SET groupNo = " + str(groupNo) + " WHERE vk_id = %s"
        for u in users:
            dbCursor.execute(query, (u))
        dbConnection.commit()
        sendMessage(vk, id, "Пользователи добавлены в группу № " + str(groupNo), keyboard=keyboards["admin"])
        del cmdNow[id]
        groupUsersSent.remove(id)


def getNotLikedPosts(id):
    global vk2
    res = []
    posts = findPost(custom="senderUserID != "+str(id)+" AND TO_DAYS(NOW()) - TO_DAYS(DATE(date)) = 0 ORDER BY `posts`.`date` DESC")
    for post in posts:
        likes = vk2.likes.getList(type="post", owner_id=post["user_id"], item_id=post["post_id"])["items"]
        if not id in likes:
            res.append(post["ID"])
    return res


def sendNotLiked():
    global dbCursor, vk
    dbCursor.execute("SELECT * FROM `users` WHERE groupNo != -1")
    for u in dbCursor.fetchall():
        e = MyEvent("", u["vk_id"])
        performTask(vk, e, False)


def banLazy():
    global dbCursor, dbConnection, vk
    dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = 0")
    for u in dbCursor.fetchall():
        sendMessage(vk, u["vk_id"], "Вы заблокированы за малую активность!", keyboard=keyboards["empty"])
    dbCursor.execute("UPDATE `users` SET groupNo = -1 WHERE groupNo = 0")
    dbConnection.commit()


def sendBestAndLazy():
    global dbCursor, vk
    lazyNum = dbCursor.execute("SELECT vk_id FROM `users` WHERE groupNo = 0")
    res = "Ленивцы: " + str(lazyNum) + "\n"
    for u in dbCursor.fetchall():
        user = vk2.users.get(user_ids=u["vk_id"])[0]
        res += user["first_name"] + " " + user["last_name"] + " (" + str(u["vk_id"]) + ")\n"
    bestNum = dbCursor.execute("SELECT vk_id, groupNo FROM `users` WHERE isBestInGroup = 1")
    res += "Лучшие в группах: " + str(bestNum) + "\n"
    for u in dbCursor.fetchall():
        user = vk2.users.get(user_ids=u["vk_id"])[0]
        res += str(u["groupNo"]) + " - " + user["first_name"] + " " + user["last_name"] + " (" + str(u["vk_id"]) + ")\n"
    for a in admins:
        sendMessage(vk, a, res)
    return res


def groupEvenly():
    global dbCursor, dbConnection
    ok = True
    j = 0
    dbCursor.execute("SELECT MAX(groupNo) AS groupNo FROM `users`")
    maxGroupNo = dbCursor.fetchall()[0]["groupNo"]
    while ok:
        if dbCursor.execute("SELECT ID, groupNo FROM `users` WHERE groupNo = %s", (maxGroupNo - j)) < usersPerGroup:
            users = dbCursor.fetchall()
            i = 1
            for u in users:
                if i == maxGroupNo:
                    i = 1
                dbCursor.execute("UPDATE `users` SET groupNo = %s WHERE ID = %s", (i, u["ID"]))
                i += 1
        else:
            ok = False
        j += 1
    dbConnection.commit()


adminCommands = {
    "Запустить обязательное задание": sendPost,
    "Создать группу": manuallyCreateGroup,
    "Заблокировать пользователя": banUser,
    "Разблокировать пользователя": unblockUser,
    "Статистика": sendAdminStats,
    "Показать пользователей": sendAdminUserList
}

commands = {
    "Отправить пост": sendPost,
    "Выполнить ежедневное задание": performTask,
    "Далее": performTask,
    "Статистика": sendStats
}
cmdNow = {}


def getCommand(text, uid):
    if uid in cmdNow:
        return cmdNow[uid]
    if text == "Отмена":
        if uid in cmdNow: del cmdNow[uid]
        if uid in waitingSend: del waitingSend[uid]
        if uid in postSent: postSent.remove(uid)
        if uid in taskSent: del taskSent[uid]
        return None
    if text in commands:
        return commands[text]
    else:
        return None


def getAdminCmd(text, uid):
    if uid in cmdNow:
        return cmdNow[uid]
    if text == "Отмена":
        if uid in cmdNow: del cmdNow[uid]
        return None
    if text in adminCommands:
        return adminCommands[text]
    else:
        return None


def safeRun(cmd, vk, event):
    try:
        cmd(vk, event)
    except Exception as e:
        print("Error, while executing command for text: " + event.text + ": " + str(e))
        sendMessage(vk, event.user_id, "Ошибка")


def onMessage(vk, event):
    if event.user_id is None:
        return
    id = event.user_id
    if id in admins:
        cmd = getAdminCmd(event.text, id)
        if cmd is not None:
            threading.Thread(target=safeRun, args=(cmd, vk, event)).start()
        else:
            sendMessage(vk, id, "Выберите действие", keyboard=keyboards["admin"])
        return
    users = findUser(event.user_id)
    if len(users) == 0:
        if id in cmdNow: del cmdNow[id]
        start(vk, event)
        return
    if users[0]["groupNo"] == -1:
        sendMessage(vk, id, "Вы заблокированы", keyboard=keyboards["empty"])
        return
    cmd = getCommand(event.text, id)
    if cmd is not None:
        threading.Thread(target=safeRun, args=(cmd, vk, event)).start()
    else:
        sendMessage(vk, id, "Выберите действие", keyboard=getKeyboard4user(id))


def schedule_pending():
    while True:
        schedule.run_pending()
        time.sleep(1)


vk_session = auth_group(token)
vk_session2 = auth_user(login, password)
if vk_session is None:
    print("Cannot log in")
    sys.exit(-1)
else:
    print("Authorization successful!")
if vk_session2 is None:
    print("Cannot log as user")
    sys.exit(-1)
else:
    print("Authorization as user successful!")

dbConnection, dbCursor = connectDB(db_host, db_user, db_password, db_dbName)

loadKeyboards()
longpoll = VkLongPoll(vk_session)
vk = vk_session.get_api()
vk2 = vk_session2.get_api()
schedule.every().day.at(
    str(perfTskTimeMin.hour)+":"+str(perfTskTimeMin.minute - (2 if perfTskTimeMin.minute != 0 else 0))
).do(groupAndDistribute)
schedule.every().day.at(
    str(perfTskTimeMax.hour)+":"+str(perfTskTimeMax.minute)
).do(giveFines)  # дать штрафы
schedule.every().day.at(
    str(perfTskTimeMax.hour) + ":" + str(perfTskTimeMax.minute + (2 if perfTskTimeMax.minute <= 58 else 0))
).do(sendStatsAll)
schedule.every().day.at(
    "20:00"
).do(sendNotLiked)
schedule.every().wednesday.at(
    "23:48"
).do(checkLikedPercent)
schedule.every().wednesday.at(
    "23:52"
).do(getBestUsersInGroups)
schedule.every().wednesday.at(
    "23:54"
).do(getLazy)
schedule.every().wednesday.at(
    "23:56"
).do(banLazy)
schedule.every().wednesday.at(
    "23:58"
).do(sendBestAndLazy)
schedule.every().monday.at(
    "0:01"
).do(clearStats)
threading.Thread(target=schedule_pending).start()
for event in longpoll.listen():
    if event.type == VkEventType.MESSAGE_NEW and event.to_me:
        onMessage(vk, event)
