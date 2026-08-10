from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def mask_card_number(card_number):
    """Masks a card number, showing only the last 4 digits."""
    if card_number and len(card_number) >= 4:
        return "**** **** **** " + card_number[-4:]
    return "****"


def get_risk_badge_color(risk_level):
    """Returns a color hex code based on risk level for the email badge."""
    colors = {
        "CRITICAL": "#b91c1c",
        "HIGH": "#d9534f",
        "MEDIUM": "#f0ad4e",
        "LOW": "#5cb85c",
    }
    return colors.get(str(risk_level).upper(), "#6c757d")


def create_fraud_card_alert_email_body(alert_data):
    """Creates the HTML email body for a fraud card usage alert."""
    masked_card = mask_card_number(alert_data['card_number'])
    risk_color = get_risk_badge_color(alert_data['risk_level'])
    international_label = "Yes" if alert_data['is_international'] else "No"

    return f"""<html><body style="font-family: Arial, sans-serif;">
    <h2 style="color: #b91c1c;">🚨 Fraud Card Activity Detected</h2>
    <p>Dear {alert_data['customer_name']},</p>
    <p>We have detected a transaction on your card that matches our fraud watchlist.
    Immediate attention may be required.</p>

    <div style="background-color: #fff3f3; padding: 15px; border-left: 4px solid #b91c1c; margin: 20px 0;">
    <h3 style="margin-top: 0;">Fraud Alert Details</h3>
    <ul style="list-style-type: none; padding-left: 0;">
    <li><strong>Alert ID:</strong> {alert_data['alert_id']}</li>
    <li><strong>Alert Type:</strong> {alert_data['alert_type']}</li>
    <li><strong>Risk Level:</strong>
        <span style="background-color: {risk_color}; color: white; padding: 2px 8px;
        border-radius: 4px; font-size: 12px;">{alert_data['risk_level']}</span>
    </li>
    <li><strong>Alert Time:</strong> {alert_data['alert_timestamp']}</li>
    </ul></div>

    <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #6c757d; margin: 20px 0;">
    <h3 style="margin-top: 0;">Card &amp; Transaction Details</h3>
    <ul style="list-style-type: none; padding-left: 0;">
    <li><strong>Card Number:</strong> {masked_card}</li>
    <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
    <li><strong>Amount:</strong> {alert_data['currency']} {alert_data['amount']:,.2f}</li>
    <li><strong>Transaction Type:</strong> {alert_data['transaction_type']}</li>
    <li><strong>Payment Channel:</strong> {alert_data['payment_channel']}</li>
    <li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
    <li><strong>Location:</strong> {alert_data['transaction_city']}, {alert_data['transaction_country']}</li>
    <li><strong>International:</strong> {international_label}</li>
    <li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
    <li><strong>Status:</strong> {alert_data['transaction_status']}</li>
    </ul></div>

    <div style="background-color: #fff8e1; padding: 15px; border-left: 4px solid #f0ad4e; margin: 20px 0;">
    <h3 style="margin-top: 0;">Watchlist Information</h3>
    <ul style="list-style-type: none; padding-left: 0;">
    <li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
    <li><strong>Watch Type:</strong> {alert_data['watch_type']}</li>
    <li><strong>Reason Code:</strong> {alert_data['reason_code']}</li>
    <li><strong>Reason:</strong> {alert_data['reason_description']}</li>
    <li><strong>Recommended Action:</strong> {alert_data['action']}</li>
    <li><strong>Reported By:</strong> {alert_data['reported_by']} ({alert_data['reported_source']})</li>
    <li><strong>Watchlist Active Since:</strong> {alert_data['watchlist_effective_from']}</li>
    </ul></div>

    <p><strong>What should you do?</strong></p>
    <ul>
    <li>If you did NOT authorize this transaction, contact our fraud department immediately.</li>
    <li>Consider temporarily blocking your card via the app or by calling customer support.</li>
    <li>If you authorized this transaction, you may disregard this notice.</li>
    </ul>
    <p>Our team is actively monitoring your account for any further suspicious activity.</p>
    <hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
    <p style="font-size: 12px; color: #6c757d;">
    Alert ID: {alert_data['alert_id']}<br>
    This is an automated message from FraudWatch Fraud Detection System.
    </p>
    </body></html>"""


# Get configuration outside the foreach_batch_sink for serialization
EMAIL_FROM = "dinuravimukthi22@gmail.com"
try:
    APP_PASSWORD = dbutils().secrets().get("fraudwatch-scope", "gmail-api-key")
except Exception as e:
    print(f"❌ Failed to retrieve Gmail API key from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="fraud_card_alert_email_notifier_sink")
def send_fraud_card_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for fraud card activity."""

    if APP_PASSWORD is None:
        print(f"❌ Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return

    rows = df.collect()
    print(f"📧 Batch {batch_id}: Processing {len(rows)} fraud card alert(s)...")

    success_count = 0
    failure_count = 0

    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'alert_type': row.alert_type,
                'alert_timestamp': str(row.alert_timestamp),
                'customer_name': row.customer_name,
                'card_number': row.card_number,
                'transaction_id': row.transaction_id,
                'amount': float(row.amount),
                'currency': row.currency,
                'transaction_type': row.transaction_type,
                'payment_channel': row.payment_channel,
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_city': row.transaction_city,
                'transaction_country': row.transaction_country,
                'transaction_timestamp': str(row.transaction_timestamp),
                'transaction_status': row.transaction_status,
                'is_international': row.is_international,
                'watchlist_id': row.watchlist_id,
                'watch_type': row.watch_type,
                'risk_level': row.risk_level,
                'action': row.action,
                'reason_code': row.reason_code,
                'reason_description': row.reason_description,
                'watchlist_effective_from': str(row.watchlist_effective_from),
                'reported_by': row.reported_by,
                'reported_source': row.reported_source,
            }

            subject = f"🚨 Fraud Card Alert [{alert_data['risk_level']}] - {alert_data['alert_id']}"
            body = create_fraud_card_alert_email_body(alert_data)

            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)

            success_count += 1
            print(f"  ✅ Email sent to {row.customer_email} for card alert {row.alert_id}")

        except Exception as e:
            failure_count += 1
            print(f"  ❌ Error processing fraud card alert {row.alert_id}: {e}")

    print(f"📊 Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")


@dp.append_flow(target="fraud_card_alert_email_notifier_sink")
def fraud_card_alert_stream():
    """Streaming flow that reads fraud card alerts and feeds the email notifier sink."""
    return spark.readStream.table("fraudwatch.gold.fraud_card_alerts")