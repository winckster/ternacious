from email.mime.text import MIMEText
import requests
from retry import retry
from retry_requests import retry as retry_request
import smtplib
from selenium import webdriver
from selenium.webdriver.common.by import By

from coordinates import BOUNDS

from config import GARMIN_USER_NAME, GARMIN_USER_ID, SENDER, RECIPIENTS, PASSWORD, GARMIN_MESSAGE_ID, GARMIN_EMAIL


def get_inreach_position():
    session = retry_request(requests.Session(), retries=5, backoff_factor=0.2)
    res = session.get(f"https://share.garmin.com/{GARMIN_USER_NAME}/Map/Messages/?userId={GARMIN_USER_ID}&typeRestriction=1&units=2")
    res.raise_for_status()
    messages = res.json()["Messages"]

    # Messages are ordered by descending timestamp.
    # Filter out messages that are track summaries and do not include lat long.
    for message in messages:
        latitude = message.get("Latitude")
        if latitude:
            break
    longitude = message["Longitude"]

    return latitude, longitude


def get_forecast_location(latitude, longitude):
    in_bounds = None
    for location, (southeast, northwest) in BOUNDS.items():
        if latitude > southeast[0] and latitude < northwest[0] and longitude < southeast[1] and longitude > northwest[1]:
            in_bounds = location
            break
    return in_bounds

@retry(tries=5, delay=30, backoff=4)
# Retry after 30, 120, 480, 1920 seconds
def _submit_form(driver, message):
    driver.get(f"https://explore.garmin.com/textmessage/txtmsg?extId={GARMIN_MESSAGE_ID}&adr={GARMIN_EMAIL}")
    driver.set_window_size(1920, 1080)
    driver.find_element(By.ID, "ReplyMessage").send_keys(message)
    driver.find_element(By.ID, "sendBtn").click()

def send_browser(message):
    options = webdriver.FirefoxOptions()
    options.add_argument("-headless")
    driver = webdriver.Firefox(options=options)
    try:
        _submit_form(driver, message)
    finally:
        driver.quit()

def send_email(subject, body, recipients=RECIPIENTS, sender=SENDER, password=PASSWORD):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
