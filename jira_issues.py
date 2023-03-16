"""A script that querying  JIRA issues by filter, doing some logic, generating message and sending it to the Slack channel   """
import logging
import logging.handlers
import os
from jira import JIRA
from slack_sdk.webhook import WebhookClient

key=os.getenv("JIRA_KEY")
slack_url=os.getenv("SLACK_URL")
slack_alert=os.getenv("SLACK_ALERT")

jira = JIRA('https://hostname/jira/', token_auth=(key))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

with open("message.txt",'r+',encoding='utf-8') as file:
    file.truncate(0)

def send_slack_alert_message(text, blocks):
    webhook_alert = WebhookClient(slack_alert)
    response = webhook_alert.send(text=text, blocks=blocks)

def check_general():
    try:
        jql_general='filter=0456'
        issues_general = (jira.search_issues(jql_general))
        general_size=len(issues_general)
        return issues_general,general_size
    except Exception as e:
            send_slack_alert_message(text="Problem occured due general issues collection", blocks=None)
            logger.error(e)

def check_abstract():
    try:
        jql_abstract='filter=046 and labels=abstract'
        jql_abstract2='filter=333 and labels=general and summary !~ Weekly'
        jql_abstract3='filter=7309 and labels=multiple'
        issues_abstract = jira.search_issues(jql_abstract)
        issues_abstract2=jira.search_issues(jql_abstract2)
        issues_abstract3=jira.search_issues(jql_abstract3)
        abstract_size=len(issues_abstract)+len(issues_abstract3)
        return issues_abstract,issues_abstract2
    except Exception as e:
            send_slack_alert_message(text="Problem occured due abstract issues collection", blocks=None)
            logger.error(e)

def create_message(general_size,issues_general,issues_abstract,issues_abstract2):
    try:
        with open("message.txt",'a',encoding='utf-8') as f:
            header='Hello, '
            f.write(header)
            if general_size!=0:
                f.write("**<General>**\n")
                for issue in (issues_general):
                    f.write(str(issue)+" ")
                    f.write(str(issue.fields.summary)+'\n')

            if issues_abstract!=[] or issues_abstract2!=[]:
                f.write("\n**<Abstract>**\n")
                for issue in issues_abstract:
                    f.write(str(issue)+" ")
                    f.write(str(issue.fields.summary)+'\n'+'\n')
                for issue3 in issues_abstract2:
                    f.write(str(issue3)+" ")
                    f.write(str(issue3.fields.summary)+'\n'+'\n')

            elif issues_general==[] and issues_abstract==[]:
                f.write("*Nothing special"+'\n ----------'+'\n'+'\n')
    except Exception as e:
        send_slack_alert_message(text="There is a problem in template creation", blocks=None)
        logger.error(e)

def send_message():
    try:
        with open("message.txt",'r+',encoding='utf-8') as f:
            message=f.read()
            webhook = WebhookClient(slack_url)
            webhook.send(
                text="fallback",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f'{message}'
                        }
                    }
                ]
            )
    except Exception as e:
        send_slack_alert_message(text="Haven't send the message to Slack", blocks=None)
        logger.error(e)

if __name__ == "__main__":
    logger.info("Started to collect general issues...")
    issues_general,general_size=check_general()
    logger.info("Finished. Collecting abstract issues...")
    issues_abstract,issues_abstract2=check_abstract()
    logger.info("Done.")
    logger.info("Issue collection is done, starting to build the message template....")
    create_message(general_size,issues_general,issues_abstract,issues_abstract2)
    logger.info("Done, sending the message to Slack channel")
    send_message()
    logger.info("Finish.")
