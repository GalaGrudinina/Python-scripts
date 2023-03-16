 """The Python script to retrieve Slack id by given user email and send the message in channel mentioning the user """
import json
import logging
import os
import urllib3
from datetime import datetime
from slack_sdk.webhook import WebhookClient
import datetime
import os
logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)

token=os.getenv('SLACK_TOKEN')
slack_url=os.getenv('SLACK_URL')
slack_url_alert=os.getenv('SLACK_URL_ALERT')

def main():
    user_list=['gala@gmail.com', 'g@gmail.com']
    start_date = datetime.date(2023, 3, 6)

    # Number of weeks that have passed since the start date
    weeks_passed = (datetime.date.today() - start_date).days // 7

    # Index of the user who is currently on duty
    current_user_index = weeks_passed % len(user_list)
    # Name of the user who is currently on duty
    current_user = user_list[current_user_index]
    return current_user

def get_slack_member_id_using_email(current_user, token):
    """ Use the Slack Apps API to get the slack user id based on owner email id """
    http = urllib3.PoolManager()
    headers = {
        'Authorization': 'Bearer ' + token,
    }
    slack_user_id = []
    current_user = current_user.split(":")
    for email_id in current_user:
        try:
            response = http.request('GET', 'https://slack.com/api/users.lookupByEmail?email=' + email_id,
                                    headers=headers)
            json_data = json.loads(response.data.decode('utf-8'))
            slack_user_id.append(json_data['user']['id'])
            print(slack_user_id)
        except Exception as e:
            logger.error(
                "Something is not right with the Slack User API to get the Slack user ID. Please check the Email passed.")
            logger.error(e)
    return slack_user_id

def send_message(slack_user_id ):
    slack_user_id=''.join(slack_user_id)
    webhook = WebhookClient(slack_url)

    webhook.send(
        text="fallback",
        blocks=[
            {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f" <@{slack_user_id}>, it's your turn to facilitate Team time event"":coffee:"
			}
            }
        ]
    )

if __name__ == "__main__":
    try:
        current_user=main()
        slack_user_id =get_slack_member_id_using_email(current_user, token)
        send_message(slack_user_id )
    except Exception as e:
        webhook_alert = WebhookClient(slack_url_alert)
        response = webhook_alert.send(text="Oops,problem to retrive and send the message")
        logger.error(e)
