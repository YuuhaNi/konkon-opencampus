import json
import boto3
import logging
import os
import urllib.request
import time
from linebot import LineBotApi
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import random

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']  # 環境変数からテーブル名を取得
table = dynamodb.Table(table_name)

label_messages_table_name = os.environ['LABEL_MESSAGES_TABLE_NAME']
label_messages_table = dynamodb.Table(label_messages_table_name)

user_labels_table_name = os.environ['USER_LABELS_TABLE_NAME']
user_labels_table = dynamodb.Table(user_labels_table_name)

line_bot_api = LineBotApi(os.environ["CHANNEL_ACCESS_TOKEN"])

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
model_arn = os.environ['REKOGNITION_MODEL_ARN']   # カスタムラベルモデルのARN

# ラベルの一覧を定義
label_list = ["成瀬記念講堂","七十年館の食堂メニュー表","成瀬記念館分館（旧成瀬仁蔵住宅）","成瀬記念館","百年館"]

# 目標スコアを環境変数から取得 (デフォルトは30点)
goal_point = int(os.environ.get('GOAL_POINT', 30))

# テーブルスキャン
def operation_scan():
    scanData = table.scan()
    items = scanData['Items']
    print(items)
    return scanData

# レコード検索
def operation_query(user_id, timestamp):
    queryData = table.query(
        KeyConditionExpression=Key("userId").eq(user_id) & Key("timestamp").eq(timestamp)
    )
    items = queryData['Items']
    print(items)
    return items

# レコード追加・更新
def operation_put(user_id, timestamp, display_name, message_text, image_url=None):
    putResponse = table.update_item(
        Key={
            'userId': user_id,
            'timestamp': timestamp
        },
        UpdateExpression='SET #dn = :dn, #mt = :mt, #iu = :iu',
        ExpressionAttributeNames={
            '#dn': 'display_name',
            '#mt': 'message_text',
            '#iu': 'image_url'
        },
        ExpressionAttributeValues={
            ':dn': display_name,
            ':mt': message_text,
            ':iu': image_url
        },
        ReturnValues='UPDATED_NEW'
    )
    if putResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(putResponse)
    else:
        print('PUT Successed.')
    return putResponse

# レコード削除
def operation_delete(user_id, timestamp):
    delResponse = table.delete_item(
        Key={
            'userId': user_id,
            'timestamp': timestamp
        }
    )
    if delResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(delResponse)
    else:
        print('DEL Successed.')
    return delResponse

# ラベル名とメッセージのマッピングを取得する関数
def get_label_message(label):
    try:
        response = label_messages_table.query(
            KeyConditionExpression=Key('label').eq(label)
        )
        if 'Items' in response:
            return response['Items'][0].get('message')
    except Exception as e:
        print(f"Error retrieving label message: {e}")
    return None

# ラベル名とスコアのマッピングを取得する関数
def get_label_score(label):
    try:
        response = label_messages_table.query(
            KeyConditionExpression=Key('label').eq(label)
        )
        if 'Items' in response:
            return response['Items'][0].get('score', 0)  # デフォルト値を0に設定
        else:
            print(f"Label '{label}' has no score")
            return 0
    except Exception as e:
        print(f"Error retrieving label score: {e}")
        return 0

# ユーザーのラベル判別履歴とスコアを取得する関数
def get_user_labels_and_scores(user_id):
    try:
        response = user_labels_table.query(
            KeyConditionExpression=Key('userId').eq(user_id)
        )
        labels_and_scores = [(item['label'], item['score']) for item in response['Items']]
    except Exception as e:
        print(f"Error retrieving user label history and scores: {e}")
        labels_and_scores = []
    return labels_and_scores

def put_user_label(user_id, label, score):
    try:
        user_labels_table.put_item(
            Item={
                'userId': user_id,
                'label': label,
                'score': score
            }
        )
    except Exception as e:
        print(f"Error putting user label: {e}")

def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event))
    print("Received event: " + json.dumps(event))

    # LINEのリクエストからeventを取得する
    for message_event in json.loads(event["body"])["events"]:
        logger.info(json.dumps(message_event))

        if "replyToken" in message_event:
            user_id = message_event["source"]["userId"]
            timestamp = int(time.time())  # 現在のUNIXタイムスタンプを整数で取得

            # ユーザーの表示名を取得
            profile = line_bot_api.get_profile(user_id)
            display_name = profile.display_name

            if message_event["message"]["type"] == "image":
                print("画像がきた")  # 画像が来たことを出力

                # 画像処理を実行
                message_content = line_bot_api.get_message_content(message_event["message"]["id"])
                image_data = message_content.content

                # 日本時間を取得
                jp_time = datetime.utcfromtimestamp(timestamp) + timedelta(hours=9)
                date = jp_time.strftime('%y-%m-%d-%H-%M-%S')  # 日時の形式を変更
                file_name = f"{date}.jpg"  # ファイル名の形式を日時.jpgに変更
                bucket_name = os.environ['BUCKET_NAME']  # 環境変数からバケット名を取得

                # 画像をS3バケットに保存
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=image_data)
                print("Successfully stored in S3.")

                # 画像の分類
                response = rekognition.detect_custom_labels(
                    Image={'S3Object': {'Bucket': bucket_name, 'Name': file_name}},
                    ProjectVersionArn=model_arn
                )

                # 分類結果の取得
                labels = response['CustomLabels']
                if len(labels) > 0:
                    top_label = labels[0]['Name']
                    confidence = labels[0]['Confidence']
                    reply_text = f"この画像は{display_name}さんがキャンパスで撮った{top_label}ってところかな？ (信頼度: {confidence:.2f}%)"
                    logger.info(f"Image classification result: {top_label} (Confidence: {confidence:.2f}%)")  # CloudWatchに分類結果を出力

                    # ラベルに対応するメッセージとスコアをDBから取得
                    label_message = get_label_message(top_label)
                    label_score = get_label_score(top_label)

                    # ユーザーのラベル判別履歴にtop_labelとスコアを追加
                    put_user_label(user_id, top_label, label_score)

                    reply_text += f"\n{label_score}ポイント獲得！"

                    # 分類結果のメッセージを作成
                    reply_messages = [
                        {
                            "type": "text",
                            "text": reply_text
                        }
                    ]

                    if label_message:
                        # DBから取得したメッセージを追加
                        reply_messages.append({
                            "type": "text",
                            "text": label_message
                        })

                    # クイックリプライボタンを作成
                    quick_reply_buttons = [
                        {
                            "type": "action",
                            "action": {
                                "type": "message",
                                "label": "撮って欲しい場所は？",
                                "text": "撮って欲しい場所は？"
                            }
                        }
                    ]

                    # ユーザーのラベル判別履歴を取得
                    user_labels_and_scores = get_user_labels_and_scores(user_id)

                    # 判別履歴があれば、クイックリプライボタンに追加
                    if user_labels_and_scores:
                        quick_reply_buttons.append({
                            "type": "action",
                            "action": {
                                "type": "message",
                                "label": "判別履歴",
                                "text": "判別履歴"
                            }
                        })

                    # クイックリプライを含むメッセージを返信メッセージに追加
                    reply_messages.append({
                        "type": "text",
                        "text": "もっといろんな場所の写真が見たいな✨",
                        "quickReply": {
                            "items": quick_reply_buttons
                        }
                    })

                    # メッセージを一度に送信
                    send_reply_message(message_event["replyToken"], reply_messages, user_id)
                else:
                    txt = ["インスタ映えする素敵な場所だね😍",
                            "くんくん...素敵なところだね🌈",
                            f"すごくいい写真だね✨\n僕もこの写真の場所に行ってみたいな！"]
                    reply_text = random.choice(txt)
                    logger.info("Image classification result: Others")  # CloudWatchに分類結果を出力
                    reply_messages = [
                        {
                            "type": "text",
                            "text": reply_text
                        }
                    ]
                    send_reply_message(message_event["replyToken"], reply_messages, user_id)

                # 画像のURLをDBに保存
                image_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
                operation_put(user_id, timestamp, display_name, None, image_url)
            else:
                # テキストメッセージの処理
                message_text = message_event["message"]["text"]
                operation_put(user_id, timestamp, display_name, message_text)

                # 返信メッセージを作成
                reply_messages = [
                    {
                        "type": "text",
                        "text": f"{display_name}さん、画像を送ってほしいな！！"
                    }
                ]

                # クイックリプライボタンを作成
                quick_reply_buttons = [
                    {
                        "type": "action",
                        "action": {
                            "type": "message",
                            "label": "撮って欲しい場所は？",
                            "text": "撮って欲しい場所は？"
                        }
                    }
                ]

                # ユーザーのラベル判別履歴を取得
                user_labels_and_scores = get_user_labels_and_scores(user_id)

                # 判別履歴があれば、クイックリプライボタンに追加
                if user_labels_and_scores:
                    quick_reply_buttons.append({
                        "type": "action",
                        "action": {
                            "type": "message",
                            "label": "判別履歴",
                            "text": "判別履歴"
                        }
                    })

                # クイックリプライを含むメッセージを返信メッセージに追加
                reply_messages.append({
                    "type": "text",
                    "text": "画像を判別するよ！",
                    "quickReply": {
                        "items": quick_reply_buttons
                    }
                })

                # クイックリプライの応答を処理
                if message_event["message"]["text"] == "撮って欲しい場所は？":
                    # 上記で作成したreply_messagesを上書きする
                    label_text = f"今回{display_name}さんに撮って欲しいオススメの場所はこちら！\n" + "\n".join(["・" + label for label in label_list])
                    reply_messages = [
                        {
                            "type": "text",
                            "text": label_text
                        }
                    ]
                elif message_event["message"]["text"] == "判別履歴":
                    user_labels_and_scores = get_user_labels_and_scores(user_id)
                    if user_labels_and_scores:
                        unique_labels = list(set(label for label, _ in user_labels_and_scores))
                        label_text = f"{display_name}さんからもらった画像で判定できた場所はこちら！\n"
                        label_text += "\n".join(["・" + label for label in unique_labels])

                        # スコアを合計して変数pointに格納
                        point = sum(score for _, score in user_labels_and_scores)
                        label_text += f"\n\n現在の{display_name}さんの合計ポイントは{point}点だよ。"
                        if point >= goal_point:
                            label_text += f"\nわぁ！{goal_point}ポイントをこえたね！\nたくさん写真を送ってくれてありがとう✨"

                    else:
                        label_text = "まだ何も写真をもらってないよ！"

                    reply_messages = [
                        {
                            "type": "text",
                            "text": label_text
                        }
                    ]

                # メッセージを送信
                send_reply_message(message_event["replyToken"], reply_messages, user_id)

    return {
        "statusCode": 200,
        "body": json.dumps("Hello from Lambda!")
    }


def send_reply_message(reply_token, messages, user_id):
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + os.environ["CHANNEL_ACCESS_TOKEN"]
    }
    data = {
        "replyToken": reply_token,
        "messages": messages
    }
    logger.info(f"Sending reply message: {data}")
    req = urllib.request.Request(url=url, data=json.dumps(data).encode("utf-8"), method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req) as res:
            logger.info(f"Response status: {res.status}")
            logger.info(f"Response body: {res.read().decode('utf-8')}")
            # ユーザーの判別履歴とスコアを取得し、一覧表示
            user_labels_and_scores = get_user_labels_and_scores(user_id)
            if user_labels_and_scores:
                unique_labels = list(set(label for label, _ in user_labels_and_scores))
                print("User labels and scores:")
                for label, score in user_labels_and_scores:
                    print(f"- {label}: {score}")
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        logger.error(f"HTTP error body: {e.read().decode('utf-8')}")